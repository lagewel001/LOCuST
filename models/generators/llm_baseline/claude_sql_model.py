from anthropic import AnthropicFoundry, APIConnectionError, APIStatusError, RateLimitError
from typing import Tuple

import config
from models.generators.llm_baseline.base_llm_generator import BaseLLMGenerator
from models.retrievers.colbert.colbert_retriever import ColBERTRetriever
from utils.global_functions import parse_bool

logger = config.logging.getLogger(__name__)


class ClaudeBaselineSQLModel(BaseLLMGenerator):
    def __init__(self, checkpoint: str, reasoning: str = 'False'):
        super().__init__()
        self.retriever = ColBERTRetriever(checkpoint=checkpoint, mode='all')
        self.reasoning = parse_bool(reasoning)

        self.model_name = "claude-sonnet-5"
        self.ml_client = AnthropicFoundry(
            base_url="<API_URL>",
            api_key="<API_KEY>",
        )

    def _call_llm(self, system_prompt: str, messages: list) -> Tuple[str, Tuple[int, int]]:
        try:
            response = self.ml_client.messages.create(
                # Cache the large, stable system prompt so repeated queries reuse it at ~0.1x
                # input cost. Verify it hits with response.usage.cache_read_input_tokens.
                system=[{
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=messages,
                model=self.model_name,
                # Adaptive thinking can consume a large share of the budget; give it headroom.
                # 16000 stays under the SDK's non-streaming HTTP-timeout threshold.
                max_tokens=16000,
                thinking={"type": "adaptive"} if self.reasoning else {"type": "disabled"},
                output_config={"effort": "high" if self.reasoning else "low"},
            )

            if response.stop_reason == "max_tokens":
                logger.warning("LLM response truncated at max_tokens; the generated query may be empty")

            num_tokens = (self._input_token_count(response.usage), response.usage.output_tokens)
            # Join text blocks by type: with adaptive thinking the response can also carry a thinking block
            text = "".join(b.text for b in response.content if b.type == "text")
            return self._extract_response(text), num_tokens
        except RateLimitError as e:
            logger.error("LLM rate limited after retries: %s", e)
        except APIStatusError as e:
            logger.error("LLM call failed (HTTP %s): %s", e.status_code, e.message)
        except APIConnectionError as e:
            logger.error("LLM connection error: %s", e)
        except Exception:
            logger.exception("Unexpected error while calling the LLM")
        return "", (0, 0)

    @staticmethod
    def _input_token_count(usage) -> int:
        """
            The full prompt size: with prompt caching, `input_tokens` only counts the uncached part, and the
            cached system prompt is reported separately as cache creation/read tokens.
        """
        return (usage.input_tokens + (getattr(usage, 'cache_creation_input_tokens', None) or 0)
                + (getattr(usage, 'cache_read_input_tokens', None) or 0))

from openai import AzureOpenAI, APIConnectionError, APIStatusError, RateLimitError
from typing import Tuple

import config
from models.generators.llm_baseline.base_llm_generator import BaseLLMGenerator
from models.retrievers.colbert.colbert_retriever import ColBERTRetriever
from utils.global_functions import parse_bool

logger = config.logging.getLogger(__name__)


class GPTBaselineSQLModel(BaseLLMGenerator):
    def __init__(self, checkpoint: str, reasoning: str = 'False'):
        super().__init__()
        self.retriever = ColBERTRetriever(checkpoint=checkpoint, mode='all')
        self.reasoning = parse_bool(reasoning)

        self.model_name = "gpt-6-sol"
        self.ml_client = AzureOpenAI(
            api_version=config.AZURE_API_VERSION,
            azure_endpoint="<BASE_URL>",
            api_key="<API_KEY>",
            # Azure returns transient 408/500s; the SDK retries these with backoff (default is only 2 retries).
            # A question that still fails ends up with an empty query, which a rerun of the evaluation regenerates.
            max_retries=8,
        )

    def _call_llm(self, system_prompt: str, messages: list) -> Tuple[str, Tuple[int, int]]:
        try:
            response = self.ml_client.chat.completions.create(
                # The system prompt goes first, so OpenAI's automatic prefix caching reuses it across queries.
                # prompt_tokens already includes the cached tokens, so no token-count correction is needed.
                messages=[{"role": "system", "content": system_prompt}, *messages],
                model=self.model_name,
                max_completion_tokens=16000,
                # Explicitly 'none': unlike gpt-5.1, gpt-6 reasons by default (between 'low' and 'medium')
                reasoning_effort="high" if self.reasoning else "none",
            )

            choice = response.choices[0]
            if choice.finish_reason == "length":
                logger.warning("LLM response truncated at max_completion_tokens; the generated query may be empty")
            elif choice.finish_reason == "content_filter":
                logger.warning("LLM response blocked by the content filter")

            num_tokens = (response.usage.prompt_tokens, response.usage.completion_tokens)
            # content is None when the reasoning used up the whole budget or the content filter kicked in
            return self._extract_response(choice.message.content or ""), num_tokens
        except RateLimitError as e:
            logger.error("LLM rate limited after retries: %s", e)
        except APIStatusError as e:
            logger.error("LLM call failed (HTTP %s): %s", e.status_code, e.message)
        except APIConnectionError as e:
            logger.error("LLM connection error: %s", e)
        except Exception:
            logger.exception("Unexpected error while calling the LLM")
        return "", (0, 0)

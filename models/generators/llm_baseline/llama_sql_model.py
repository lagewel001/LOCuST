from openai import OpenAI, APIConnectionError, APIStatusError, RateLimitError
from typing import Tuple

import config
from models.generators.llm_baseline.base_llm_generator import BaseLLMGenerator
from models.retrievers.colbert.colbert_retriever import ColBERTRetriever
from utils.global_functions import parse_bool

logger = config.logging.getLogger(__name__)


class LlamaBaselineSQLModel(BaseLLMGenerator):
    def __init__(self, checkpoint: str, reasoning: str = 'False'):
        super().__init__()
        self.retriever = ColBERTRetriever(checkpoint=checkpoint, mode='all')
        self.reasoning = parse_bool(reasoning)
        if self.reasoning:
            # Maverick is not a reasoning model: an OpenAI-compatible endpoint rejects or ignores reasoning_effort
            logger.warning("Llama 4 Maverick does not support reasoning; ignoring the reasoning flag")

        self.model_name = "Llama-4-Maverick-17B-128E-Instruct-FP8"
        self.ml_client = OpenAI(
            base_url="<BASE_URL>",
            api_key="<API_KEY>",
            # Azure returns transient 408/500s; the SDK retries these with backoff (default is only 2 retries).
            # A question that still fails ends up with an empty query, which a rerun of the evaluation regenerates.
            max_retries=8,
        )

    def _call_llm(self, system_prompt: str, messages: list) -> Tuple[str, Tuple[int, int]]:
        try:
            response = self.ml_client.chat.completions.create(
                messages=[{"role": "system", "content": system_prompt}, *messages],
                model=self.model_name,
                max_completion_tokens=16000,
            )

            choice = response.choices[0]
            if choice.finish_reason == "length":
                logger.warning("LLM response truncated at max_completion_tokens; the generated query may be empty")

            num_tokens = (response.usage.prompt_tokens, response.usage.completion_tokens)
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

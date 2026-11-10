import os
from openai import OpenAI, APIConnectionError, APIStatusError
from typing import Tuple

import config
from models.generators.llm_baseline.base_llm_generator import BaseLLMGenerator
from models.retrievers.colbert.colbert_retriever import ColBERTRetriever
from utils.global_functions import parse_bool

logger = config.logging.getLogger(__name__)


class QwenBaselineSQLModel(BaseLLMGenerator):
    """
        Qwen baseline, served locally by vLLM through its OpenAI-compatible API. Start the server with the
        reasoning parser, so the thinking is split off from message.content:

            VLLM_USE_FLASHINFER_SAMPLER=0 CUDA_VISIBLE_DEVICES=0 vllm serve Qwen/Qwen3.8-27B --port 8001 \
              --max-model-len 131072 --reasoning-parser qwen3 --language-model-only \
              --gpu-memory-utilization 0.92 --max-num-seqs 16 --api-key <api-key>

        The client is configured with QWEN_BASE_URL, QWEN_MODEL (must equal the served model name) and
        QWEN_API_KEY (only checked when vLLM was started with --api-key).
    """
    def __init__(self, checkpoint: str, reasoning: str = 'False'):
        super().__init__()
        self.retriever = ColBERTRetriever(checkpoint=checkpoint, mode='all')
        self.reasoning = parse_bool(reasoning)

        self.model_name = "Qwen/Qwen3.8-27B"
        self.ml_client = OpenAI(
            # Port 8001: vLLM's default port 8000 is the EndpointRetriever's default as well
            base_url="http://localhost:8001/v1",
            # The SDK requires a key, even when the server does not check it
            api_key="<API_KEY>",
            # 16k tokens of thinking on a 27B model can take longer than the SDK's default of 600 s
            timeout=1800,
        )

    def _call_llm(self, system_prompt: str, messages: list) -> Tuple[str, Tuple[int, int]]:
        try:
            response = self.ml_client.chat.completions.create(
                # The system prompt goes first, so vLLM's automatic prefix caching reuses it across queries
                messages=[{"role": "system", "content": system_prompt}, *messages],
                model=self.model_name,
                # vLLM rejects (HTTP 400) a request whose prompt + max_completion_tokens exceeds its --max-model-len
                max_completion_tokens=16000,
                # Qwen ignores reasoning_effort and thinks by default: thinking is switched in the chat template
                extra_body={"chat_template_kwargs": {"enable_thinking": self.reasoning}},
            )

            choice = response.choices[0]
            if choice.finish_reason == "length":
                logger.warning("LLM response truncated at max_completion_tokens; the generated query may be empty")

            # completion_tokens includes the thinking tokens
            num_tokens = (response.usage.prompt_tokens, response.usage.completion_tokens)
            # content is None when the thinking used up the whole budget. Keeping only the text after the last
            # </think> is a safety net for a server started without --reasoning-parser: the thinking would then
            # end up in content, and _extract_response would pick up a draft query from it.
            content = (choice.message.content or "").rsplit("</think>", 1)[-1]
            return self._extract_response(content), num_tokens
        except APIStatusError as e:
            logger.error("LLM call failed (HTTP %s): %s", e.status_code, e.message)
        except APIConnectionError as e:
            # Also covers timeouts (APITimeoutError), e.g. when the vLLM server is not running or still loading
            logger.error("LLM connection error: %s", e)
        except Exception:
            logger.exception("Unexpected error while calling the LLM")
        return "", (0, 0)

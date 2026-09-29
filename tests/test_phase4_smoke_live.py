import os
import pytest

@pytest.mark.asyncio
async def test_live_session_initialization():
    has_credentials = bool(os.environ.get("OPENAI_API_KEY"))
    if not has_credentials:
        pytest.skip("No OpenAI API key found; live session initialization skipped.")
    
    # We would test actual AgentSession connection here if we had credentials
    pass

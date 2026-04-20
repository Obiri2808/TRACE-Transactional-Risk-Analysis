import os
import openai

def create_vocareum_openai_client():
    """Create an OpenAI client configured for Vocareum routing."""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError('OPENAI_API_KEY not set')
    return openai.OpenAI(base_url='https://openai.vocareum.com/v1', api_key=api_key)

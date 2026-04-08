from ollama import chat

stream = chat(
    model='gemma3:4b',
    messages=[{'role': 'user', 'content': 'What is 17 × 23?'}],
    stream=True,
)

in_thinking = False
content = ''
thinking = ''
for chunk in stream:
    if chunk.message.thinking:
        if not in_thinking:
            in_thinking = True
            print('Thinking:\n', end='', flush=True)
        print(chunk.message.thinking, end='', flush=True)
        # accumulate the partial thinking
        thinking += chunk.message.thinking
    elif chunk.message.content:
        if in_thinking:
            in_thinking = False
            print('\n\nAnswer:\n', end='', flush=True)
        print(chunk.message.content, end='', flush=True)
        # accumulate the partial content
        content += chunk.message.content

    # append the accumulated fields to the messages for the next request
    new_messages = [{"role": 'assistant', "thinking": thinking, "content": content}]

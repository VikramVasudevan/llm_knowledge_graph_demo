import os
import json
from arcadedb_utils import (
    get_all_characters_table_from_arcade,
    get_all_scriptures_table_arcade,
    get_all_topics_table_arcade,
    get_perspectives_from_graph_arcade,
    get_verses_by_scripture_arcade,
    get_verses_for_character_from_arcade,
    get_verses_for_topic_arcade,
)
from dotenv import load_dotenv
from gradio.components.chatbot import ExampleMessage
from openai import OpenAI
import gradio as gr

# --- 1. Setup & Environment ---
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# Global cache to keep the UI in sync with the Database
TOPIC_TO_NODES_MAP = {}

# --- 2. Data Fetchers ---
def get_all_characters_table(search_query=""):
    return get_all_characters_table_from_arcade(search_query=search_query)

def get_verses_for_character(evt: gr.SelectData):
    return get_verses_for_character_from_arcade(evt=evt)

def get_all_scriptures_table():
    return get_all_scriptures_table_arcade()

def get_verses_by_scripture(evt: gr.SelectData, scripture_data, filter_mode):
    return get_verses_by_scripture_arcade(evt=evt, scripture_data=scripture_data, filter_mode=filter_mode)

def get_top_10_topics():
    topics = get_all_topics_table_arcade()
    top_10 = sorted(topics, key=lambda x: x[1], reverse=True)[:28]
    return [ExampleMessage({"text": t[0]}) for t in top_10]

def get_verses_for_topic(evt: gr.SelectData):
    return get_verses_for_topic_arcade(evt=evt)

def get_all_topics_table(search_query=""):
    return get_all_topics_table_arcade(search_query=search_query)

# --- 3. Chat Logic ---
def get_perspectives_from_graph(user_query, history, use_fts=True):
    return get_perspectives_from_graph_arcade(client=client, user_query=user_query, conversation_history=history, use_fts=use_fts)

def bhashyam_chat(message, history, use_fts):
    try:
        context, identified_topics = get_perspectives_from_graph(message, history, use_fts)
        if not context:
            yield "🔍 No matching verses found."
            return

        formatted_context = ""
        for c in context:
            formatted_context += f"\nFROM [{c['scripture']}] ({c['verse']}): {c['verse_text']} : {c['meaning']}\n"

        system_prompt = f"""
            You are the Bhashyam AI Research Assistant, a scholar of Sanatana Dharma scriptures.
            Use the 'CONTEXT FROM GRAPH' provided below to answer the user's query.
            Focus on citations and original text.
            ### CONTEXT FROM GRAPH:
            {formatted_context}
            """
        stream = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": message},
            ],
            stream=True,
            temperature=0.2
        )
        partial_message = ""
        for chunk in stream:
            if chunk.choices[0].delta.content:
                partial_message += chunk.choices[0].delta.content
                yield partial_message
    except Exception as e:
        yield f"⚠️ Error: {str(e)}"

# --- 4. User Tabbed UI ---
with gr.Blocks() as demo:
    gr.HTML("<h1 style='text-align: center;'>🕉️ Bhashyam AI - Research</h1>")

    with gr.Tabs():
        # --- Tab 1: Chat ---
        with gr.Tab("💬 Research Chat"):
            chatbot = gr.Chatbot(height=600, show_label=False, examples=get_top_10_topics())
            with gr.Row():
                msg = gr.Textbox(placeholder="Ask your query...", label="Ask Bhashyam", scale=9)
                submit_btn = gr.Button("Send", variant="primary", scale=1)
                fts_toggle = gr.Checkbox(label="Enable FTS", value=True, visible=False)

        # --- Tab 2: Search By Topic ---
        with gr.Tab("🏷️ Search By Topic"):
            with gr.Row():
                with gr.Column(scale=1):
                    topics_table = gr.Dataframe(headers=["Topic Name", "Verse Count"], value=get_all_topics_table(), interactive=False, show_search="search")
                with gr.Column(scale=2):
                    detail_header = gr.Markdown("### 📖 Topic Details\n*Select a topic.*")
                    verse_detail_table = gr.Dataframe(headers=["Scripture", "Verse ID", "Original Text", "English Translation", "Word-by-Word"], interactive=False, show_search="search")

        # --- Tab 3: Search By Character ---
        with gr.Tab("🎭 Search By Character"):
            with gr.Row():
                with gr.Column(scale=1):
                    chars_table = gr.Dataframe(headers=["Character Name", "Mentions"], value=get_all_characters_table(), interactive=False, show_search="search")
                with gr.Column(scale=2):
                    char_detail_header = gr.Markdown("### 📖 Character Details\n*Select a character.*")
                    char_verse_table = gr.Dataframe(headers=["Scripture", "Verse ID", "Original Text", "English Translation", "Word-by-Word"], interactive=False, show_search="search")

        # --- Tab 4: Search By Scripture ---
        with gr.Tab("📜 Search By Scripture"):
            with gr.Row():
                with gr.Column(scale=1):
                    scripture_table = gr.Dataframe(headers=["Scripture Title", "Verses", "Enrichment", "internal_id"], value=get_all_scriptures_table(), interactive=False, show_search="search")
                with gr.Column(scale=2):
                    scripture_detail_header = gr.Markdown("### 📖 Scripture Content\n*Select a scripture.*")
                    scripture_verse_table = gr.Dataframe(headers=["Verse ID", "Original Text", "English Translation", "Word-by-Word", "Topics", "Characters", "Global Id"], interactive=False, show_search="search")

    # --- Events ---
    def user_action(user_message, history):
        return "", history + [{"role": "user", "content": user_message}]

    def bot_action(history):
        user_message = history[-1]["content"]
        bot_response = bhashyam_chat(user_message, history, True)
        history.append({"role": "assistant", "content": ""})
        for chunk in bot_response:
            history[-1]["content"] = chunk
            yield history

    # NEW: Handler for when a user clicks a Chatbot Example
    def handle_example_click(evt: gr.SelectData, history):
        # evt.value is the dictionary: {"text": "TopicName"}
        topic_name = evt.value["text"]
        user_message = (
            f"What do our sanatana dharma scriptures say on the topic of '{topic_name}'"
        )
        _, updated_history = user_action(user_message, history)
        return updated_history

    # Bind the 'Send' button and 'Enter' key
    submit_btn.click(user_action, [msg, chatbot], [msg, chatbot]).then(
        bot_action, [chatbot], chatbot
    )
    msg.submit(user_action, [msg, chatbot], [msg, chatbot]).then(
        bot_action, [chatbot], chatbot
    )
    # Bind example click
    chatbot.example_select(handle_example_click, chatbot, chatbot).then(
        bot_action, [chatbot], chatbot
    )
    
    topics_table.select(fn=get_verses_for_topic, outputs=[detail_header, verse_detail_table])
    chars_table.select(fn=get_verses_for_character, outputs=[char_detail_header, char_verse_table])
    
    # Updated Scripture Select: Remove view_mode_toggle
    def select_scripture_helper(evt: gr.SelectData, scripture_data):
        return get_verses_by_scripture_arcade(evt=evt, scripture_data=scripture_data, filter_mode="Show All")

    scripture_table.select(fn=select_scripture_helper, inputs=[scripture_table], outputs=[scripture_detail_header, gr.Markdown(), scripture_verse_table])

if __name__ == "__main__":
    demo.queue().launch(share=False)

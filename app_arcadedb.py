import os
import json
from arcadedb_utils import get_all_characters_table_from_arcade, get_all_scriptures_table_arcade, get_all_topics_table_arcade, get_enrichment_stats_arcade, get_perspectives_from_graph_arcade, get_verses_by_scripture_arcade, get_verses_for_character_from_arcade, get_verses_for_topic_arcade, update_topic_everywhere_arcade
from dotenv import load_dotenv
from gradio.components.chatbot import ExampleMessage
from openai import OpenAI
import gradio as gr
import re
import sqlite3

# --- 1. Setup & Environment ---
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)

# Global cache to keep the UI in sync with the Database
TOPIC_TO_NODES_MAP = {}


def get_all_characters_table(search_query=""):
    return get_all_characters_table_from_arcade(search_query=search_query)


def get_verses_for_character(evt: gr.SelectData):
    return get_verses_for_character_from_arcade(evt=evt)


def get_all_scriptures_table():
    return get_all_scriptures_table_arcade()


def get_verses_by_scripture(evt: gr.SelectData, scripture_data, filter_mode):
    return get_verses_by_scripture_arcade(evt=evt,scripture_data=scripture_data, filter_mode=filter_mode)


def update_topic_everywhere(old_name, new_name):
    return update_topic_everywhere_arcade(old_name=old_name, new_name=new_name)


def get_top_10_topics():
    topics = get_all_topics_table()
    # Sort by count descending and take top 10
    top_10 = sorted(topics, key=lambda x: x[1], reverse=True)[:28]

    # Format for gr.Chatbot: List of lists of message dicts
    # Each 'example' is actually a full conversation start
    return [ExampleMessage({"text": t[0]}) for t in top_10]


def get_enrichment_stats():
    return get_enrichment_stats_arcade()


def get_verses_for_topic(evt: gr.SelectData):
    return get_verses_for_topic_arcade(evt=evt)


def get_all_topics_table(search_query=""):
    return get_all_topics_table_arcade(search_query=search_query)


# --- 3. Original Perspectives & Chat Logic ---
def get_perspectives_from_graph(user_query, use_fts=True):
    return get_perspectives_from_graph_arcade(client=client, user_query=user_query, use_fts=use_fts)


def bhashyam_chat(message, history, use_fts):
    try:
        context, identified_topics = get_perspectives_from_graph(message, use_fts)
        print("context:\n", context)
        print("identified_topics:\n", identified_topics)
        if not context:
            yield "🔍 No matching verses found."
            return

        formatted_context = ""
        for c in context:
            formatted_context += f"\nFROM [{c['scripture']}] ({c['verse']}): {c['verse_text']} : {c['meaning']}\n"

        system_prompt = f"""
            You are the Bhashyam AI Research Assistant, a scholar of Sanatana Dharma scriptures.
            Use the 'CONTEXT FROM GRAPH' provided below to answer the user's query.

            ### RULES:
            1. COMPARISON: Since you have references from different scriptures, highlight similarities or nuances between them in your summary.
            2. CITATION: Always cite the [Scripture Name] and [Verse Title].
            3. LYRICS: For every verse you reference, you MUST include the original text (Lyrics/Sanskrit/Tamil) followed by its translation.
            4. ATOMICITY: Focus on the specific philosophical topics linked to these verses.

            ### RESPONSE FORMAT:
            - Comparative summary of the concept across the provided scriptures.
            - [Scripture Name] [Verse Title]
            - **Original Text:** [Insert Verse Text here]
            - **Meaning:** [Insert English Translation here]
            
            ### CONTEXT FROM GRAPH:
            {formatted_context}
            """
        stream = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": message},
            ],
            stream=True,
        )
        partial_message = ""
        for chunk in stream:
            if chunk.choices[0].delta.content:
                partial_message += chunk.choices[0].delta.content
                yield partial_message
    except Exception as e:
        yield f"⚠️ Error: {str(e)}"


# --- 4. Tabbed UI ---

with gr.Blocks() as demo:
    gr.HTML("<h1 style='text-align: center;'>🕉️ Bhashyam AI - Research Suite</h1>")

    with gr.Tabs():
        # --- Tab 1: Chat ---
        with gr.Tab("💬 Research Chat"):
            with gr.Row():
                with gr.Column(scale=4):
                    # Set type="messages" for compatibility with dict-based history
                    chatbot = gr.Chatbot(
                        height=600,
                        show_label=False,
                        placeholder="### 🔥 Trending Topics",
                        examples=get_top_10_topics(),
                    )
                    with gr.Row():
                        msg = gr.Textbox(
                            placeholder="Ask your query...",
                            label="Ask Bhashyam",
                            scale=9,
                        )
                        submit_btn = gr.Button("Send", variant="primary", scale=1)

                with gr.Column(scale=1, variant="panel"):
                    gr.Markdown("### 🛠️ Search Settings")
                    fts_toggle = gr.Checkbox(
                        label="Enable Full-Text Search",
                        value=True,
                        info="Disable to test pure Graph lookups.",
                    )
                    stats_sidebar = gr.Markdown(get_enrichment_stats())
                    refresh_stats_btn = gr.Button("🔄 Refresh All Stats")

        # --- Tab 2: Topics Index ---
        with gr.Tab("🏷️ Topics Index"):
            with gr.Row():
                with gr.Column(scale=2):
                    gr.Markdown("### 🔍 Filter & Browse")

                    # Added a dedicated Refresh button for the Topic Table
                    refresh_topics_btn = gr.Button("🔄 Refresh Topic List", size="sm")

                    # --- RENAME PANEL ---
                    with gr.Accordion("⚙️ Rename Topic", open=False):
                        old_name_input = gr.Textbox(
                            label="Selected Topic", interactive=False
                        )
                        new_name_input = gr.Textbox(
                            label="New Name", placeholder="Type new name..."
                        )
                        rename_btn = gr.Button(
                            "Apply Rename Everywhere", variant="stop", interactive=False
                        )
                        rename_status = gr.Markdown("")

                    topics_table = gr.Dataframe(
                        headers=["Topic Name", "Verse Count"],
                        datatype=["str", "number"],
                        value=get_all_topics_table(),
                        interactive=False,
                        show_search="search",
                        column_widths=[200, 80],
                    )
                with gr.Column(scale=4):
                    detail_header = gr.Markdown(
                        "### 📖 Topic Details\n*Select a topic on the left to view verses.*"
                    )
                    verse_detail_table = gr.Dataframe(
                        headers=[
                            "Scripture",
                            "Verse ID",
                            "Original Text",
                            "English Translation",
                            "Word-by-Word",
                        ],
                        datatype=["str", "str", "str", "str", "str"],
                        wrap=True,
                        interactive=False,
                        # Redistributed: Metadata (20% total), Content (80% total)
                        column_widths=["10%", "10%", "25%", "25%", "30%"],
                    )

        # --- Tab 2.5: Character Index ---
        with gr.Tab("🎭 Character Index"):
            with gr.Row():
                with gr.Column(scale=2):
                    gr.Markdown("### 🔍 Search Characters")
                    refresh_chars_btn = gr.Button(
                        "🔄 Refresh Character List", size="sm"
                    )

                    chars_table = gr.Dataframe(
                        headers=["Character Name", "Mentions"],
                        datatype=["str", "number"],
                        value=get_all_characters_table(),
                        interactive=False,
                        show_search="search",
                        column_widths=[200, 80],
                    )
                with gr.Column(scale=4):
                    char_detail_header = gr.Markdown(
                        "### 📖 Character Details\n*Select a character on the left to view verses.*"
                    )
                    char_verse_table = gr.Dataframe(
                        headers=[
                            "Scripture",
                            "Verse ID",
                            "Original Text",
                            "English Translation",
                            "Word-by-Word",
                        ],
                        datatype=["str", "str", "str", "str", "str"],
                        wrap=True,
                        interactive=False,
                        column_widths=["10%", "10%", "25%", "25%", "30%"],
                    )

        # --- Tab 3: Scripture Index ---
        with gr.Tab("📜 Scripture Index"):
            with gr.Row():
                with gr.Column(scale=2):
                    gr.Markdown("### 📚 Browse by Scripture")
                    refresh_scripture_btn = gr.Button("🔄 Refresh List", size="sm")

                    scripture_table = gr.Dataframe(
                        headers=[
                            "Scripture Title",
                            "Verses",
                            "Enrichment",
                            "internal_id",
                        ],
                        datatype=["str", "number", "str", "str"],
                        value=get_all_scriptures_table(),
                        interactive=False,
                        show_search="search",
                        column_widths=["55%", "15%", "30%", "0%"],
                    )

                with gr.Column(scale=4):
                    scripture_detail_header = gr.Markdown(
                        "### 📖 Scripture Content\n*Select a scripture on the left.*"
                    )

                    # --- ADDED TOGGLE HERE ---
                    view_mode_toggle = gr.Radio(
                        choices=["Show All", "Pending Enrichment Only"],
                        value="Show All",
                        label="View Mode",
                        info="Filter verses missing Topics, WBW, or Translation",
                    )

                    scripture_enrichment_stats = gr.Markdown(
                        "Select a scripture to see enrichment progress."
                    )
                    scripture_verse_table = gr.Dataframe(
                        headers=[
                            "Verse ID",
                            "Original Text",
                            "English Translation",
                            "Word-by-Word",
                            "Topics",
                            "Characters",  # Added Header
                            "Global Id",
                        ],
                        datatype=[
                            "str",
                            "str",
                            "str",
                            "str",
                            "str",
                            "str",
                            "str",
                        ],  # Added str for Character
                        wrap=True,
                        interactive=False,
                        show_search="search",
                        # Balanced widths: ID(10), Text(15), Trans(20), WBW(20), Topics(15), Chars(15), Global(5)
                        column_widths=["10%", "15%", "20%", "20%", "15%", "15%", "5%"],
                    )
    # --- Event Bindings ---

    # Refresh Statistics Sidebar
    refresh_stats_btn.click(get_enrichment_stats, outputs=stats_sidebar)

    # Refresh Topics Table
    refresh_topics_btn.click(fn=lambda: get_all_topics_table(), outputs=topics_table)

    rename_btn.click(
        fn=update_topic_everywhere,
        inputs=[old_name_input, new_name_input],
        outputs=rename_status,
    ).then(
        fn=lambda: get_all_topics_table(),  # Refresh the table automatically
        outputs=topics_table,
    )

    # 1. Update the helper to return all 4 necessary components
    def select_topic_for_rename(evt: gr.SelectData):
        # Get the clean topic name
        topic_name = evt.value if isinstance(evt.value, str) else evt.value[0]

        # Call the existing verse fetcher logic
        header, details = get_verses_for_topic(evt)

        # Return:
        # 1. Topic name for 'old_name_input'
        # 2. Empty string for 'new_name_input' (to clear previous typing)
        # 3. Header markdown for 'detail_header'
        # 4. Verse list for 'verse_detail_table'
        return topic_name, "", header, details

    # 2. Rebind the select event to the new helper
    # Make sure to remove the old topics_table.select(fn=get_verses_for_topic...)
    # and replace it with this:
    topics_table.select(
        fn=select_topic_for_rename,
        outputs=[old_name_input, new_name_input, detail_header, verse_detail_table],
    )

    # --- Chatbot Event Logic ---
    def user_action(user_message, history):
        # New format: list of dicts
        return "", history + [{"role": "user", "content": user_message}]

    def bot_action(history, use_fts):
        user_message = history[-1]["content"]
        bot_response = bhashyam_chat(user_message, history, use_fts)
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
        # We reuse the user_action logic to add it to history
        _, updated_history = user_action(user_message, history)
        return updated_history

    # Bind the 'Send' button and 'Enter' key
    submit_btn.click(user_action, [msg, chatbot], [msg, chatbot]).then(
        bot_action, [chatbot, fts_toggle], chatbot
    )
    msg.submit(user_action, [msg, chatbot], [msg, chatbot]).then(
        bot_action, [chatbot, fts_toggle], chatbot
    )
    chatbot.example_select(handle_example_click, chatbot, chatbot).then(
        bot_action, [chatbot, fts_toggle], chatbot
    )
    # Refresh Scripture List
    refresh_scripture_btn.click(fn=get_all_scriptures_table, outputs=scripture_table)

    # Scripture Table Selection Logic
    scripture_table.select(
        fn=get_verses_by_scripture,
        inputs=[scripture_table, view_mode_toggle],  # Added toggle input
        outputs=[
            scripture_detail_header,
            scripture_enrichment_stats,
            scripture_verse_table,
        ],
    )

    # Refresh Character Table
    refresh_chars_btn.click(fn=lambda: get_all_characters_table(), outputs=chars_table)

    # Character Table Selection Logic
    chars_table.select(
        fn=get_verses_for_character,
        outputs=[char_detail_header, char_verse_table],
    )

if __name__ == "__main__":
    demo.queue().launch(
        theme=gr.themes.Default(primary_hue="orange", secondary_hue="gray"), share=False
    )

#![allow(non_snake_case)]

use dioxus::prelude::*;
use serde::{Deserialize, Serialize};
use serde_wasm_bindgen::from_value;
use wasm_bindgen::prelude::*;

static CSS: Asset = asset!("/assets/styles.css");
static TAURI_ICON: Asset = asset!("/assets/tauri.svg");
static DIOXUS_ICON: Asset = asset!("/assets/dioxus.png");

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"])]
    async fn invoke(cmd: &str, args: JsValue) -> JsValue;
}

#[derive(Serialize, Deserialize)]
struct GreetArgs<'a> {
    name: &'a str,
}

async fn fetch_contexts() -> Vec<String> {
    let js_value = invoke("contexts", JsValue::default()).await;
    from_value(js_value).unwrap_or_else(|_| vec![])
}

pub fn App() -> Element {
    let contexts = use_resource(fetch_contexts);
    let mut selected_context = use_signal(|| "Choose a context...".to_string());

    rsx! {
        div {
            style: "padding: 20px; font-family: sans-serif;",
            h3 { "Select a Context:" }

            match &*contexts.read_unchecked() {
                Some(list) => rsx! {
                    select {
                        onchange: move |evt| selected_context.set(evt.value()),

                        option {
                            value: "",
                            disabled: true,
                            selected: true,
                            "{selected_context}"
                        }

                        for item in list {
                            option {
                                value: "{item}",
                                "{item}"
                            }
                        }
                    }

                    if !selected_context().is_empty() {
                        p { "You selected: {selected_context}" }
                    }
                },
                None => rsx! {
                    p { "Loading options from Rust backend..." }
                }
            }
        }
    }
    // rsx! {
    //     link { rel: "stylesheet", href: CSS }
    //     main {
    //         class: "container",
    //         h1 { "Welcome to Tauri + Dioxus + Phenoxtract!" }
    //         p { "{contexts}" }
    //         div {
    //             class: "row",
    //             a {
    //                 href: "https://tauri.app",
    //                 target: "_blank",
    //                 img {
    //                     src: TAURI_ICON,
    //                     class: "logo tauri",
    //                      alt: "Tauri logo"
    //                 }
    //             }
    //             a {
    //                 href: "https://dioxuslabs.com/",
    //                 target: "_blank",
    //                 img {
    //                     src: DIOXUS_ICON,
    //                     class: "logo dioxus",
    //                     alt: "Dioxus logo"
    //                 }
    //             }
    //         }
    //         p { "Click on the Tauri and Dioxus logos to learn more." }
    //
    //         form {
    //             class: "row",
    //             onsubmit: greet,
    //             input {
    //                 id: "greet-input",
    //                 placeholder: "Enter a name...",
    //                 value: "{name}",
    //                 oninput: move |event| name.set(event.value())
    //             }
    //             button { r#type: "submit", "Greet" }
    //         }
    //         p { "{greet_msg}" }
    //     }
    // }
}

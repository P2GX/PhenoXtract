<script lang="ts">
import { type DialogFilter, open } from "@tauri-apps/plugin-dialog";
import { FolderOpenSolid } from "flowbite-svelte-icons";

/**
 * A text input paired with a native OS file/directory picker button,
 * powered by Tauri's dialog plugin.
 *
 * @example
 * ```svelte
 * <FilePicker
 *   directory
 *   multiple={false}
 *   placeholder="Choose a folder…"
 *   bind:value={chosenPath}
 * />
 * ```
 */
interface Props {
  directory: boolean;
  multiple: boolean;
  placeholder: string;
  filters?: DialogFilter[];
  value?: string;
}
let { directory, multiple, placeholder, filters, value = $bindable("") }: Props = $props();

async function triggerFileSelect() {
  try {
    const selected = await open({
      directory: directory,
      multiple: multiple,
      filters: filters,
    });

    if (selected !== null) {
      value = selected as string;
    }
  } catch (error) {
    console.error("Failed to open directory picker:", error);
  }
}
</script>

<div class="input-wrapper">
  <input id="dir-input" title="Directory Text Box" {placeholder} bind:value />
  <button
    id="pick-dir-button"
    type="button"
    aria-label="Open Directory"
    title="Browse"
    onclick={triggerFileSelect}
  >
    <FolderOpenSolid />
  </button>
</div>

<style>
.input-wrapper {
  position: relative;
  display: inline-block;
}

input {
  border-radius: 8px;
  border: 1px solid #0f0f0f;
  padding: 0.6em 2.5em 0.6em 1.2em;
  font-size: 1em;
  font-weight: 500;
  font-family: inherit;
  color: #0f0f0f;
  background-color: #ffffff;
  transition: border-color 0.25s;
  box-sizing: border-box;
}

#pick-dir-button {
  position: absolute;
  right: 0.5em;
  top: 50%;
  transform: translateY(-50%);
  background: none;
  border: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 0;
  color: #666;
  transition: color 0.2s;
}

#pick-dir-button:hover {
  color: #0f0f0f;
}
</style>

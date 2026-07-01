<script>
import { FolderOpenSolid } from "flowbite-svelte-icons";

let directory = $state("");
</script>

<div class="input-wrapper">
  <input id="dir-input" placeholder="Pick Dir..." bind:value={directory} />
  <button id="pick-dir-button" type="button" aria-label="Open Directory" title="Browse">
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

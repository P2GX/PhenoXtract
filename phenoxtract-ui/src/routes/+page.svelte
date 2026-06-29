<script lang="ts">
import { Button, GradientButton } from "flowbite-svelte";
import { Navbar, NavLi, NavHamburger, NavUl, Search, ToolbarButton } from "flowbite-svelte";
import GithubLogo from "../icons/GithubLogo.svelte";
import BugIcon from "../icons/BugIcon.svelte";
import EmailIcon from "../icons/EmailIcon.svelte";
import logo from "../icons/px-logo-2.png";
// import { invoke } from "@tauri-apps/api/core";

import { SearchOutline } from "flowbite-svelte-icons";
import flattenColorPalette from "tailwindcss/lib/util/flattenColorPalette";

let fileInput: HTMLInputElement;
let selectedFolder = $state<string>("");

function handleChange(event: Event) {
  const target = event.target as HTMLInputElement;
  if (target.files && target.files.length > 0) {
    selectedFolder = target.files[0].webkitRelativePath.split("/")[0];
  }
}

let name = $state("");
</script>

<div class="app-container">
  <aside class="sidebar">
    <header class="brand">
      <img alt="PX Logo" src={logo} class="logo" />
      <div class="brand-text">
        <h1>Phenoxtract</h1>
        <span>0.1.0</span>
      </div>
    </header>

    <Button class="m-2.5 bg-gray-500 justify-start">Projects</Button>
    <Button class="bg-transparent m-2.5 hover:bg-gray-500 justify-start">Settings</Button>
    <Button class="bg-transparent m-2.5 hover:bg-gray-500 justify-start">Documentation</Button>

    <footer>
      <a href="https://github.com/P2GX/PhenoXtract" target="_blank"
        ><GithubLogo size="50px" color="#000000FF" strokeWidth="0" /></a
      >
      <a href="https://github.com/P2GX/PhenoXtract/issues" target="_blank"
        ><BugIcon size="50px" color="#000000FF" strokeWidth="0" /></a
      >
      <a href="mailto:Rouven.Reuter@bih-charite.de" target="_blank"
        ><EmailIcon size="50px" color="#000000FF" strokeWidth="0" /></a
      >
    </footer>
  </aside>

  <div class="content-wrapper">
    <Navbar class="border-b border-gray-500">
      {#snippet children({toggle})}
        <div class="flex">
          <ToolbarButton class="block items-center md:hidden" onclick={toggle}>
            <SearchOutline class="h-5 w-5 text-gray-500 dark:text-gray-400" />
          </ToolbarButton>
          <div class="hidden md:block">
            <Search size="md" placeholder="Search..." />
          </div>
          <NavHamburger class="bg-gray-500 text-white" />
        </div>

        <NavUl>
          <NavLi class="bg-gray-500  text-white text-center mr-5" href="/"
            ><p class="px-2.5">New Project</p></NavLi
          >
          <input
            type="file"
            bind:this={fileInput}
            webkitdirectory
            class="hidden"
            onchange={handleChange}
          />

          <NavLi onclick={() => fileInput.click()} class="bg-gray-500  text-white text-center mr-5">
            <p class="px-2.5">Open</p>
          </NavLi>
        </NavUl>
      {/snippet}
    </Navbar>
    <div class="main-content">
      <button
        type="button"
        class="bg-transparent hover:bg-gray-500 w-full h-24 text-left text-white p-3 rounded flex items-center gap-4"
      >
        <div class="square">ID</div>
        <div>
          <h3>Immunology Data</h3>
          <span class="text-sm opacity-75">~/some/path/to/project</span>
        </div>
      </button>
      <button
        type="button"
        class="bg-transparent hover:bg-gray-500 w-full h-24 text-left text-white p-3 rounded flex items-center gap-4"
      >
        <div class="square">ID</div>
        <div>
          <h3>Immunology Data</h3>
          <span class="text-sm opacity-75">~/some/path/to/project</span>
        </div>
      </button>
      <button
        type="button"
        class="bg-transparent hover:bg-gray-500 w-full h-24 text-left text-white p-3 rounded flex items-center gap-4"
      >
        <div class="square">ID</div>
        <div>
          <h3>Immunology Data</h3>
          <span class="text-sm opacity-75">~/some/path/to/project</span>
        </div>
      </button>
      <button
        type="button"
        class="bg-transparent hover:bg-gray-500 w-full h-24 text-left text-white p-3 rounded flex items-center gap-4"
      >
        <div class="square">ID</div>
        <div>
          <h3>Immunology Data</h3>
          <span class="text-sm opacity-75">~/some/path/to/project</span>
        </div>
      </button>
      <button
        type="button"
        class="bg-transparent hover:bg-gray-500 w-full h-24 text-left text-white p-3 rounded flex items-center gap-4"
      >
        <div class="square">ID</div>
        <div>
          <h3>Immunology Data</h3>
          <span class="text-sm opacity-75">~/some/path/to/project</span>
        </div>
      </button>
    </div>
  </div>
</div>

<style>
footer {
  display: flex;
  flex: 0 0 auto;
  padding: 1rem;
  background: #262626;
  margin-top: auto;
  flex-direction: row;
  place-items: center;
  gap: 0.5rem;
}
.square {
  width: 25px;
  height: 25px;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #ff3e00;
  color: white;
  font-family: "RussOne", serif;
  font-size: 0.8rem;
  border-radius: 5px;
}

.sidebar {
  width: 250px;
  border-right: 1px solid #6a7282;
  display: flex;
  flex-direction: column;
  padding-top: 1rem;
  background-color: #262626;
  height: 100%;
}

.brand {
  display: flex;
  align-items: center;
  padding: 0 1.5rem 2rem 1.5rem;
  gap: 0.75rem;
}

.brand-text h1 {
  font-size: 1.5rem;
  margin: 0;
  font-weight: normal;
  font-family: "RussOne", serif;
  color: #ffffff;
}

.brand-text span {
  font-size: 0.75rem;
  color: #c1c1c1;
}

.app-container {
  display: flex;
  height: 100vh;
  width: 100vw;
  overflow: hidden;
}

.content-wrapper {
  flex: 1;
  display: flex;
  flex-direction: column; /* Stacks Navbar on top, Main Content on bottom */
  height: 100%;
  overflow: hidden;
}

.main-content {
  flex: 1; /* Fills all remaining vertical space under the Navbar */
  overflow-y: auto; /* Adds a scrollbar only here if your content is long */
  background-color: #262626; /* Or whatever color you want your main area */
  padding: 1.5rem;
}

:root {
  font-family: Inter, Avenir, Helvetica, Arial, sans-serif;
  font-size: 16px;
  line-height: 24px;
  font-weight: 400;

  color: #0f0f0f;
  background-color: #262626;

  font-synthesis: none;
  text-rendering: optimizeLegibility;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  -webkit-text-size-adjust: 100%;
}

.logo {
  height: 2.5em;
  will-change: filter;
  transition: 0.75s;
}

a {
  font-weight: 500;
  color: #646cff;
  text-decoration: inherit;
}

a:hover {
  color: #535bf2;
}

h1 {
  text-align: center;
}

#greet-input {
  margin-right: 5px;
}

@media (prefers-color-scheme: dark) {
  :root {
    color: #f6f6f6;
    background-color: #2f2f2f;
  }

  a:hover {
    color: #24c8db;
  }

  input,
  button {
    color: #ffffff;
    background-color: #0f0f0f98;
  }

  button:active {
    background-color: #0f0f0f69;
  }
}
</style>

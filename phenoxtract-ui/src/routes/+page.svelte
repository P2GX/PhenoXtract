<script lang="ts">
    import {invoke} from "@tauri-apps/api/core";

    import {Navbar, NavLi, NavUl, NavHamburger, Search, ToolbarButton} from "flowbite-svelte";
    import {SearchOutline} from "flowbite-svelte-icons";

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


<Navbar>
    {#snippet children({toggle})}
        <div class="flex">
            <ToolbarButton class="block items-center md:hidden" onclick={toggle}>
                <SearchOutline class="h-5 w-5 text-gray-500 dark:text-gray-400"/>
            </ToolbarButton>
            <div class="hidden md:block">
                <Search size="md" placeholder="Search..."/>
            </div>
            <NavHamburger class="bg-gray-500 text-white"/>

        </div>

        <NavUl>
            <NavLi class="bg-gray-500  text-white text-center mr-5" href="/"><p class="px-2.5"> New Project </p></NavLi>
            <input
                    type="file"
                    bind:this={fileInput}
                    webkitdirectory
                    class="hidden"
                    onchange={handleChange}
            />

            <NavLi onclick={() => fileInput.click()} class="bg-gray-500  text-white text-center mr-5">
                <p class="px-2.5"> Open</p>
            </NavLi>
        </NavUl>
    {/snippet}
</Navbar>
<hr class="h-px mt-2 border-0 bg-gray-500 m-5"/>
<div class="w-px h-60 bg-gray-500 mx-3"></div>


<style>
    .logo.vite:hover {
        filter: drop-shadow(0 0 2em #747bff);
    }

    .logo.svelte-kit:hover {
        filter: drop-shadow(0 0 2em #ff3e00);
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

    .container {
        margin: 0;
        display: flex;
        flex-direction: column;
        justify-content: center;
        text-align: center;
    }

    .logo {
        height: 6em;
        padding: 1.5em;
        will-change: filter;
        transition: 0.75s;
    }

    .logo.tauri:hover {
        filter: drop-shadow(0 0 2em #24c8db);
    }

    .row {
        display: flex;
        justify-content: center;
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

    input,
    button {
        border-radius: 8px;
        border: 1px solid transparent;
        padding: 0.6em 1.2em;
        font-size: 1em;
        font-weight: 500;
        font-family: inherit;
        color: #0f0f0f;
        background-color: #ffffff;
        transition: border-color 0.25s;
        box-shadow: 0 2px 2px rgba(0, 0, 0, 0.2);
    }

    button {
        cursor: pointer;
    }

    button:hover {
        border-color: #396cd8;
    }

    button:active {
        border-color: #396cd8;
        background-color: #e8e8e8;
    }

    input,
    button {
        outline: none;
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

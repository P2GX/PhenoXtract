<script module lang="ts">
  export const colorPalette: string[] = [
    '#6b2643', '#ac2847', '#ec273f', '#94493a', '#de5d3a', '#e98537',
    '#f3a833', '#4d3533', '#6e4c30', '#a26d3f', '#ce9248', '#dab163', '#e8d282', '#f7f3b7',
    '#1e4044', '#006554', '#26854c', '#5ab552', '#9de64e', '#008b8b', '#62a477', '#a6cb96',
    '#d3eed3', '#3e3b65', '#3859b3', '#3388de', '#36c5f4', '#6dead6', '#5e5b8c', '#8c78a5',
    '#b0a7b8', '#deceed', '#9a4d76', '#c878af', '#cc99ff', '#fa6e79', '#ffa2ac', '#ffd1d5',
  ];

  function getColorFromName(name: string): string {
    if (!name) return colorPalette[0];
    let hash = 0;
    for (let i = 0; i < name.length; i++) {
      hash = name.charCodeAt(i) + ((hash << 5) - hash);
    }
    return colorPalette[Math.abs(hash) % colorPalette.length];
  }
</script>

<script lang="ts">
import type { MouseEventHandler } from "svelte/elements";

interface Props {
  name: string;
  directory: string;
  squareColor?: string;
  onClick: MouseEventHandler<HTMLButtonElement>;
  hoverColor?: string;
  bgColor?: string;
}
let { name, directory, squareColor = getColorFromName(name), onClick,  hoverColor = "var(--highlight-color)", bgColor= "transparent"}: Props = $props();

function getInitials(name: string): string {
  const words = name
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .split(/[\s\-_]+/)
    .filter(Boolean);

  if (words.length === 0) return "";
  if (words.length === 1) return words[0][0].toUpperCase();

  const first = words[0][0];
  const last = words[words.length - 1][0];

  return (first + last).toUpperCase();
}
</script>

<button type="button"
        class="project-panel"
        style="--hover-color: {hoverColor}; --bg-color: {bgColor}"
        onclick={onClick}>
  <div class="square" style="--square-color: {squareColor}">{getInitials(name)}</div>
  <div>
    <h3>{name}</h3>
    <span class="text-sm opacity-75">{directory}</span>
  </div>
</button>

<style>
.project-panel {
  background-color: var(--bg-color);
  width: 100%;
  height: 6rem;
  text-align: left;
  color: white;
  padding: 0.75rem;
  border-radius: 0.50rem;
  display: flex;
  align-items: center;
  gap: 1rem;
}

.project-panel:hover {
  background-color: var(--hover-color);
}

.square {
  width: 25px;
  height: 25px;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: var(--square-color);
  color: white;
  font-family: var(--highlight-font), serif;
  font-size: 0.8rem;
  border-radius: 5px;
}
</style>

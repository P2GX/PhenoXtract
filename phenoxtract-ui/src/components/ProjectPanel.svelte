<script lang="ts">
import type { MouseEventHandler } from "svelte/elements";

interface Props {
  name: string;
  directory: string;
  squareColor: string;
  onClick: MouseEventHandler<HTMLButtonElement>;
  hoverColor?: string;
  bgColor?: string;
}
let { name, directory, squareColor, onClick,  hoverColor = "var(--highlight-color)", bgColor= "transparent"}: Props = $props();

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

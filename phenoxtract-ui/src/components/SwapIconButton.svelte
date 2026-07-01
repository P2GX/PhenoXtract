<script lang="ts">
import type { Snippet } from "svelte";

/**
 * A button that changes its icon for the time the assinged function runs.
 *
 * @example
 * ```svelte
 *  <SwapIconButton onclick={greet} aria-label="Play or Stop">
 *     {#snippet idleIcon()}
 *       <PlaySolid width={60} height={60} color="green" class="hover:scale-110" />
 *     {/snippet}
 *
 *     {#snippet activeIcon()}
 *       <StopSolid width={60} height={60} color="red" class="animate-pulse" />
 *     {/snippet}
 *   </SwapIconButton>
 * ```
 */
interface Props {
  idleIcon: Snippet;
  activeIcon: Snippet;
  onclick: (event: MouseEvent) => Promise<void>;
  "aria-label": string;
}

let { idleIcon, activeIcon, onclick, "aria-label": ariaLabel }: Props = $props();

let isPending = $state(false);

async function handleClick(event: MouseEvent) {
  isPending = true;
  try {
    await onclick(event);
  } catch (error) {
    isPending = false;
    console.error("SwapIconButton task failed:", error);
  } finally {
    isPending = false;
  }
}
</script>

<button onclick={handleClick} type="button" disabled={isPending} aria-label={ariaLabel}>
  {#if isPending}
    {@render activeIcon()}
  {:else}
    {@render idleIcon()}
  {/if}
</button>

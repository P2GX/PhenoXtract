import { render, screen, waitFor } from "@testing-library/svelte";
import userEvent from "@testing-library/user-event";
import { createRawSnippet } from "svelte";
import { describe, expect, it, vi } from "vitest";
import SwapIconButton from "./SwapIconButton.svelte";

describe("SwapIconButton", () => {
  // 1. Create raw snippets to simulate the icons passed into the component
  const idleIcon = createRawSnippet(() => ({
    render: () => '<span data-testid="idle-icon">Idle</span>',
  }));
  const activeIcon = createRawSnippet(() => ({
    render: () => '<span data-testid="active-icon">Active</span>',
  }));

  const defaultProps = {
    idleIcon,
    activeIcon,
    "aria-label": "Test Button",
  };

  it("renders the idle state by default with correct aria-label", () => {
    render(SwapIconButton, {
      props: { ...defaultProps, onclick: vi.fn().mockResolvedValue(undefined) },
    });

    const button = screen.getByRole("button", { name: "Test Button" });

    expect(button).not.toBeDisabled();
    expect(screen.getByTestId("idle-icon")).toBeInTheDocument();
    expect(screen.queryByTestId("active-icon")).not.toBeInTheDocument();
  });

  it("swaps to active icon and disables button while promise is pending", async () => {
    const user = userEvent.setup();

    let resolvePromise!: () => void;
    const mockOnclick = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          resolvePromise = resolve;
        })
    );

    render(SwapIconButton, {
      props: { ...defaultProps, onclick: mockOnclick },
    });

    const button = screen.getByRole("button", { name: "Test Button" });

    await user.click(button);

    expect(mockOnclick).toHaveBeenCalledOnce();
    expect(button).toBeDisabled();
    expect(screen.getByTestId("active-icon")).toBeInTheDocument();
    expect(screen.queryByTestId("idle-icon")).not.toBeInTheDocument();

    resolvePromise();

    await waitFor(() => {
      expect(button).not.toBeDisabled();
      expect(screen.getByTestId("idle-icon")).toBeInTheDocument();
      expect(screen.queryByTestId("active-icon")).not.toBeInTheDocument();
    });
  });

  it("reverts to idle state even if the promise rejects (finally block test)", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const user = userEvent.setup();

    // biome-ignore lint/suspicious/noExplicitAny: Its a test, chill bro.
    let rejectPromise!: (reason?: any) => void;
    const mockOnclick = vi.fn(
      () =>
        new Promise<void>((_, reject) => {
          rejectPromise = reject;
        })
    );

    render(SwapIconButton, {
      props: { ...defaultProps, onclick: mockOnclick },
    });

    const button = screen.getByRole("button");
    await user.click(button);

    expect(button).toBeDisabled();

    rejectPromise(new Error("Tauri command failed"));

    await waitFor(() => {
      expect(button).not.toBeDisabled();
      expect(screen.getByTestId("idle-icon")).toBeInTheDocument();
    });

    consoleSpy.mockRestore();
  });
});

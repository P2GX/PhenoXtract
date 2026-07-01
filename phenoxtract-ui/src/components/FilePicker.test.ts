import { render, screen, fireEvent, waitFor } from "@testing-library/svelte";
import { open } from "@tauri-apps/plugin-dialog";
import { vi, describe, it, expect, beforeEach } from "vitest";
import FilePicker from "./FilePicker.svelte";
import "@testing-library/jest-dom/vitest";

vi.mock("@tauri-apps/plugin-dialog", () => ({
  open: vi.fn(),
}));

const mockOpen = vi.mocked(open);

const defaults = {
  directory: true,
  multiple: false,
  placeholder: "Choose a folder…",
};

beforeEach(() => vi.clearAllMocks());

describe("rendering", () => {
  it("shows the placeholder text", () => {
    render(FilePicker, { props: defaults });
    expect(screen.getByPlaceholderText("Choose a folder…")).toBeInTheDocument();
  });

  it("shows an initial value if one is passed", () => {
    render(FilePicker, { props: { ...defaults, value: "/some/initial/path" } });
    expect(screen.getByRole("textbox", { name: "Directory Text Box" })).toHaveValue(
      "/some/initial/path"
    );
  });

  it("renders the browse button", () => {
    render(FilePicker, { props: defaults });
    expect(screen.getByRole("button", { name: "Open Directory" })).toBeInTheDocument();
  });
});

describe("triggerFileSelect", () => {
  it("calls open() with the correct arguments", async () => {
    mockOpen.mockResolvedValue(null);

    render(FilePicker, {
      props: {
        ...defaults,
        filters: [{ name: "Text", extensions: ["txt"] }],
      },
    });

    await fireEvent.click(screen.getByRole("button", { name: "Open Directory" }));

    expect(mockOpen).toHaveBeenCalledWith({
      directory: true,
      multiple: false,
      filters: [{ name: "Text", extensions: ["txt"] }],
    });
  });

  it("updates the input when a path is selected", async () => {
    mockOpen.mockResolvedValue("/chosen/path");

    render(FilePicker, { props: defaults });

    await fireEvent.click(screen.getByRole("button", { name: "Open Directory" }));

    await waitFor(() => {
      expect(screen.getByRole("textbox")).toHaveValue("/chosen/path");
    });
  });

  it("leaves the value unchanged when the dialog is cancelled (null)", async () => {
    mockOpen.mockResolvedValue(null);

    render(FilePicker, { props: { ...defaults, value: "/existing/path" } });

    await fireEvent.click(screen.getByRole("button", { name: "Open Directory" }));

    await waitFor(() => {
      expect(screen.getByRole("textbox")).toHaveValue("/existing/path");
    });
  });

  it("does not crash when open() throws", async () => {
    mockOpen.mockRejectedValue(new Error("IPC error"));
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    render(FilePicker, { props: defaults });

    await fireEvent.click(screen.getByRole("button", { name: "Open Directory" }));

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith(
        "Failed to open directory picker:",
        expect.any(Error)
      );
    });

    expect(screen.getByRole("textbox")).toBeInTheDocument();

    consoleSpy.mockRestore();
  });
});

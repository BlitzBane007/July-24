import tkinter as tk
from PIL import Image, ImageTk
import io
import win32clipboard

def get_image_from_clipboard():
    win32clipboard.OpenClipboard()

    # Attempt to get the image using different clipboard formats
    try:
        clipboard_data = win32clipboard.GetClipboardData(win32clipboard.CF_DIB)
    except TypeError:
        try:
            clipboard_data = win32clipboard.GetClipboardData(win32clipboard.CF_DIBV5)
        except TypeError:
            clipboard_data = None

    win32clipboard.CloseClipboard()

    if clipboard_data:
        stream = io.BytesIO(clipboard_data)
        image = Image.open(stream)
        image.thumbnail((1920, 1080))  # Adjust the size to fit the window if needed
        image_tk = ImageTk.PhotoImage(image)
        image_label.config(image=image_tk)
        image_label.image = image_tk

# Create the main window
window = tk.Tk()
window.title("")

# Create a label to display the image
image_label = tk.Label(window)
image_label.pack(padx=10, pady=10)

# Load the image from the clipboard when the window is focused
window.bind("<FocusIn>", lambda event: get_image_from_clipboard())

# Start the main event loop
window.mainloop()

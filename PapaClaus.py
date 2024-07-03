import tkinter as tk
from PIL import Image, ImageTk
import sys
import os

# Function to get the path of the GIF when bundled with PyInstaller
def resource_path(relative_path):
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# Function to update the frame of the gif
def update(ind):
    frame = frames[ind]
    ind += 1
    if ind == frame_count:
        ind = 0
    label.configure(image=frame)
    root.after(delay, update, ind)  # Adjust the delay as needed

# Function to start moving the window
def start_move(event):
    root.x = event.x
    root.y = event.y

# Function to perform the move action
def on_move(event):
    deltax = event.x - root.x
    deltay = event.y - root.y
    x = root.winfo_x() + deltax
    y = root.winfo_y() + deltay
    root.geometry(f"+{x}+{y}")

# Function to terminate the application
def on_right_click(event):
    root.destroy()

# Initialize Tkinter root
root = tk.Tk()
root.attributes('-transparentcolor', 'SystemButtonFace')  # Make the window background transparent
root.overrideredirect(True)  # Remove window decorations
root.lift()
root.wm_attributes('-topmost', True)  # Keep the window above all others
root.geometry('100x100')  # Set the window size to 100x100 pixels

# Load the GIF file using PIL
gif_path = resource_path('SC1.gif')  # Use the function to get the correct path
image = Image.open(gif_path)

# Extract frames from the GIF
frames = []  # List to hold all frames of the gif
try:
    while True:
        image.seek(len(frames))  # Move to next frame
        photo = ImageTk.PhotoImage(image.copy().resize((100, 100)))
        frames.append(photo)
except EOFError:
    pass  # We have reached the end of the GIF file

frame_count = len(frames)
delay = int(image.info['duration'])  # Get the duration between frames

# Create a label to display the GIF
label = tk.Label(root, bd=0)
label.pack()

# Bind mouse events to the label
label.bind('<Button-1>', start_move)
label.bind('<B1-Motion>', on_move)
label.bind('<Button-3>', on_right_click)  # Bind right click to termination function

# Start the animation
update(0)

# Run the Tkinter event loop
root.mainloop()

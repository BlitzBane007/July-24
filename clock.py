import tkinter as tk
import time


def update_clock():
    current_time = time.strftime("%I:%M %p")  # Format the current time in 12-hour format without seconds
    label.config(text=current_time)
    label.after(60000, update_clock)  # Update the clock every 1 minute (60,000 milliseconds)


root = tk.Tk()
root.attributes('-fullscreen', True)  # Set the window to fullscreen
root.configure(bg='black')  # Set the background color to black

label = tk.Label(root, font=('Arial', 200), fg='grey', bg='black')  # Configure label appearance
label.pack(pady=200)  # Position the label in the center

update_clock()  # Start the clock

root.mainloop()

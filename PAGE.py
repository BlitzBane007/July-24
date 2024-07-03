from urllib.parse import urlparse
import webbrowser
import tkinter as tk


def open_feed_url():
    url = "https://datafeeds.fefundinfo.com/api/v1/Feeds/738861ad-0393-48bd-962f-494c3e845364/download?token=f762c993-2531-4a87-b5e0-3c72d2d2544b"
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.split('/')
    api = path_segments[-2]  # Extract the second-to-last segment
    feed_url = f"https://datafeeds.fefundinfo.com/feeds/feeds?id={api}"
    webbrowser.open_new_tab(feed_url)


# Create the Tkinter window
window = tk.Tk()

# Create the button
button = tk.Button(window, text="Open Feed URL", command=open_feed_url)
button.pack()


# Run the Tkinter event loop
window.mainloop()



import os
import re
import urllib.parse

def url2fname(url, extension=".html"):
    """Convert a URL to a filename by removing special characters and replacing slashes with underscores."""
    url = urllib.parse.urlparse(url)
    print(url)
    # print(url)
    filename=url.netloc + url.path if not str(url.path).endswith("/") else url.netloc + str(url.path)[:-1]
    if url.query:
        filename += str("_" + re.sub(r'[?&=]', '_', url.query)).lower()
    filename = re.sub(r'[/]', '_', filename)
    return filename + extension if not filename.endswith(extension) else filename



if __name__ == "__main__":
    # Example usage
    print(url2fname("https://example.com/path/to/resource"))
    print(url2fname("https://example.com/path/to/resource/"))
    print(url2fname("https://example.com/path/to/resource.html"))
    print(url2fname("https://example.com/path/to/resource.html?CODE=0000002752"))
    print(url2fname("https://example.com/path/to/resource.html?CODE=0000002752&id=123"))
import requests
import json

def inspect_structure(data, path="root", max_depth=3, current_depth=0):
    """
    Recursively inspects a Python object from JSON and prints a hierarchical
    view of its structure, including the type of each value.

    - For dictionaries, it lists each key and the type of its value.
    - For lists, it announces the list and inspects the *first* item.
    - It uses indentation to show the tree structure.
    - It stops at `max_depth` to keep the output concise.
    """
    
    # Stop if we've gone too deep into the tree
    if current_depth > max_depth:
        return

    indent = "  " * current_depth

    # --- Case 1: The data is a Dictionary ---
    if isinstance(data, dict):
        print(f"{indent}🔍 Dict at '{path}':")
        child_indent = "  " * (current_depth + 1)
        
        # Print the type of each key's value
        for key, value in data.items():
            value_type = type(value).__name__
            print(f"{child_indent}🔑 '{key}': <{value_type}>")

        # Add a newline for readability before diving deeper
        print("")

        # Now, recurse into the children that are dicts or lists
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                inspect_structure(value, path=f"{path}.{key}", max_depth=max_depth, current_depth=current_depth + 1)
    
    # --- Case 2: The data is a List ---
    elif isinstance(data, list):
        print(f"{indent}⛓️ List at '{path}' (contains {len(data)} items)")
        
        # If the list is not empty, inspect its first element to show the structure
        if data:
            print(f"{indent}  🔬 Inspecting the first item's structure...")
            inspect_structure(data[0], path=f"{path}[0]", max_depth=max_depth, current_depth=current_depth + 1)
        else:
            print(f"{indent}  (This list is empty)")
        print("") # Add a newline for readability

    # Simple types (str, int, bool, etc.) are not processed further, as their
    # type has already been printed by their parent dictionary.


# --- Main script execution ---
url = "https://appmagic.rocks/api/v2/search/publisher-applications?sort=downloads&united_publisher_id=24472"
print(f"Querying API: {url}\n")

try:
    response = requests.get(url)
    response.raise_for_status() 
    api_data = response.json()
    
    print("--- Detailed JSON Structure Inspection (max_depth=2) ---\n")
    # We set a max_depth to control how much detail we see.
    # max_depth=2 is great for a high-level overview.
    # Increase to 3 or 4 for more detail.
    inspect_structure(api_data, max_depth=4)

except requests.exceptions.RequestException as e:
    print(f"An error occurred with the API request: {e}")
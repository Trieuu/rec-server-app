import json
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from itertools import combinations
import re
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Set, Tuple,  Optional
ProductID = str


############# Utility function

def init_data(data_path = 'data.json'):
    # Open the file and load the JSON data
    with open(data_path, 'r') as file:
        data = json.load(file)
        
    # Extract arrays from data
    orders = data.get("orders", [])
    orders = [order for order in orders if order.get('status','') == 'Done']
    
    orders_id_list = [order['order_id'] for order in orders]
    
    order_items = data.get("order_items", [])
    order_items = [order_item for order_item in order_items if order_item.get('order_id',-1) in orders_id_list]
    
    products = data.get("products", [])
    carts = data.get('carts',[])
    
    return orders, order_items, products, carts


def extract_type(product_ids, products, type_filter = ['flower']):
    """
    Extract flower_type, occasion, and color for the given product_ids (flowers only).
    
    Args:
        product_ids (list): List of product IDs.
        products (list): List of product dictionaries.
    
    Returns:
        product_types: List of dictionaries with flower_type and color.
    """
    product_types = []
    # Iterate through each product_id
    for product_id in product_ids:
        # Find the product
        product = next((p for p in products if p["product_id"] == product_id), None)
        if product and product["type"] in type_filter and "flower_details" in product:
            product_types.append({
                'type' : product.get('type',''),
                "flower_type": product["flower_details"].get("flower_type", "N/A"),
                "color": product["flower_details"].get("color", [])
            })
    
    return product_types


def user_data_counter(user_product_types):
    type_count = {} # flower / vase
    flower_count = {} 
    color_count = {}
    
    for product_type in user_product_types:
        #print(product_type)
        type_count[product_type['type']] = type_count.get(product_type['type'], 0) + 1
        if product_type.get('flower_type', '') != '':
            flower_count[product_type['flower_type']] = flower_count.get(product_type['flower_type'], 0) + 1
            for i, color in enumerate(product_type['color']):
                color_count[color] = color_count.get(product_type['color'][i], 0) + 1
      
    type_count = {key: value for key, value in sorted(type_count.items(), key = lambda item: item[1], reverse=True)}      
    flower_count = {key: value for key, value in sorted(flower_count.items(), key = lambda item: item[1], reverse=True)}
    color_count = {key: value for key, value in sorted(color_count.items(), key = lambda item: item[1], reverse=True)}
    
    return type_count, flower_count, color_count

def _is_sellable(pid):
    p = _PRODUCT_BY_ID.get(pid)
    if not p or not p.get("available", True):
        return False
    # if flowers with options, check any option has stock > 0; otherwise use top-level stock
    fd = p.get("flower_details")
    if fd and "options" in fd:
        return any(opt.get("stock", 0) > 0 for opt in fd["options"])
    return p.get("stock", 0) > 0

############# Recommendation History

def get_orders_from_user(user_id, orders ,top_k = 1):
    """
    Get the top_k most recent orders for a given user, sorted by order_date.
    
    Args:
        user_id (int): The ID of the user.
        top_k (int): Number of latest orders to return.
        orders (list): List of order dictionaries.
    
    Returns:
        user_orders[:top_k]: List of up to top_k order dictionaries, sorted by date (most recent first).
    """
    # Filter orders by user_id
    user_orders = [order for order in orders if order["user_id"] == user_id]
    
    # Sort orders by order_date in descending order
    user_orders.sort(key=lambda x: datetime.strptime(x["order_date"], "%d/%m/%Y"), reverse=True)
    
    # Return the top_k orders (or all if fewer than top_k)
    return user_orders[:top_k]

def get_products_ids_from_orders(orders, order_items):
    """
    Extract unique product_ids from the provided orders' order_items.
    
    Args:
        orders (list): List of order dictionaries.
        order_items (list): List of order_items dictionaries.
    
    Returns:
        product_ids: Set of unique product_ids.
    """
    product_ids = set()  # Use set for uniqueness
    
    # Iterate through each order
    for order in orders:
        # Find corresponding order_items
        order_item = next((item for item in order_items if item["order_id"] == order["order_id"]), None)
        if order_item:
            # Extract product_ids from the order's products
            for product in order_item["products"]:
                product_ids.add(product["product_id"])
    
    return product_ids

def get_products_from_user_orders(user_id, orders, order_items, products, top_k, type_filter = ['flower']):
    user_orders = get_orders_from_user(user_id, orders, top_k=top_k)
    user_products_ids = get_products_ids_from_orders(user_orders, order_items)
    user_product_types = extract_type(user_products_ids, products, type_filter)
    
    return user_products_ids, user_product_types

from collections import Counter

def rec_user_his(user_id, orders, order_items, products, top_k, recommended_list ,type_filter = ['flower']):
    '''
    Recommend the flower that:
        - User not buy yet
        - Come up with one most refered attribute
        - Come up with the attribute that not occur get_preference
    
    '''
    user_products_ids ,user_product_types = get_products_from_user_orders(user_id, orders, order_items ,products, top_k, type_filter)
    
    
    _, flower_count, color_count = user_data_counter(user_product_types)
    
    
    #print(flower_count)
    #print(color_count)
    
    # rank signals
    ranked_flower = [ft for ft,_ in Counter(flower_count).most_common()]
    ranked_color  = [c for c,_ in Counter(color_count).most_common()]

    # try same flower_type with a color they haven't bought
    for ft in ranked_flower:
        for p in products:
            if p.get('flower_details',{}).get('flower_type') == ft:
                pid = p['product_id']
                new_colors = [c for c in p.get('flower_details',{}).get('color',[]) if c not in color_count]
                if new_colors and _is_sellable(pid) and pid not in user_products_ids and pid not in recommended_list:
                    recommended_list.add(pid)
                    return {'flag':'history','product_id':pid,'color':new_colors[0],'event':''}, recommended_list

    # fallback: top color they like but new product
    for c in ranked_color:
        for p in products:
            if c in p.get('flower_details',{}).get('color',[]):
                pid = p['product_id']
                if pid not in user_products_ids and _is_sellable(pid) and pid not in recommended_list:
                    recommended_list.add(pid)
                    return {'flag':'history','product_id':pid,'color':c,'event':''}, recommended_list

    # final fallback: any new flower type
    for p in products:
        pid = p['product_id']
        ft = p.get('flower_details',{}).get('flower_type','')
        if ft and ft not in flower_count and _is_sellable(pid) and pid not in recommended_list:
            recommended_list.add(pid)
            return {'flag':'history','product_id':pid,'color':'','event':''}, recommended_list

    return {'flag':'history','product_id':'','color':'','event':''}, recommended_list
        

############# Recommendation Occasion

events = {
    "New Year": "01/01",
    "Valentine's Day": "14/02",
    "International Women's Day": "08/03",
    "Easter": "22/03 - 25/04",
    "Mother's Day": "08/05 - 14/05",
    "Father's Day": "15/06 - 21/06",
    "Mid-Autumn Festival": "15/09 - 10/10",
    "Halloween": "31/10",
    "Thanksgiving": "22/11 - 28/11",
    "Christmas": "25/12",
    "Vietnamese Women's Day": "20/10",
    "Vietnamese Teacher's Day": "20/11",
    "Lunar New Year (Tết)": "20/01 - 20/02",
    "Reunification Day": "30/04",
    "Labor Day": "01/05",
    "Vietnam National Day": "02/09",
    "Chinese New Year": "21/01 - 20/02",
    "Earth Day": "22/04",
    "Summer Solstice": "20/06 - 22/06",
    "Mid-Year Sales": "01/06 - 15/07",
    "Black Friday": "23/11 - 29/11",
    "Cyber Monday": "26/11 - 02/12",
    "Singles' Day": "11/11",
    "World Environment Day": "05/06",
    "International Friendship Day": "30/07",
    "Oktoberfest": "21/09 - 06/10",
    "Winter Solstice": "20/12 - 23/12",
    "Boxing Day": "26/12",
    "Hung Kings Commemoration Day": "10/03",
    "Tet Trung Thu (Children's Festival)": "15/08",
}


def get_nearest_upcoming_event(events=events, now=None):
    """
    Find the next upcoming event (or the event currently in progress).
    - Single-day events roll to next year if already passed this year.
    - Ranged events return immediately if 'now' is within the range; otherwise
      the next range start in the future (this year or next) is considered.
    """
    if now is None:
        now = datetime.now()

    next_event = None
    min_diff = timedelta(days=10**6)  # big number

    for event_name, date_str in events.items():
        if ' - ' in date_str:
            # Handle ranges like "DD/MM - DD/MM"
            start_str, end_str = [s.strip() for s in date_str.split(' - ')]

            # Build start/end for the *current* cycle (might span year-end)
            year = now.year
            start = datetime.strptime(f"{start_str}/{year}", "%d/%m/%Y")
            end = datetime.strptime(f"{end_str}/{year}", "%d/%m/%Y")

            # If the range wraps to next year (e.g., 25/12 - 05/01)
            if end < start:
                end = end.replace(year=year + 1)

            # Case 1: currently inside the range → it's the "upcoming" event
            if start <= now <= end:
                return event_name

            # Case 2: upcoming later this year
            if now < start:
                candidate_start = start
            else:
                # already passed; consider next year's occurrence
                next_year_start = start.replace(year=start.year + 1)
                # Recompute next year's end respecting wrap logic
                candidate_start = next_year_start

            diff = candidate_start - now

        else:
            # Single-day event "DD/MM"
            year = now.year
            date_this_year = datetime.strptime(f"{date_str}/{year}", "%d/%m/%Y")
            if date_this_year < now:
                candidate = date_this_year.replace(year=year + 1)
            else:
                candidate = date_this_year
            diff = candidate - now

        if timedelta(0) <= diff < min_diff:
            min_diff = diff
            next_event = event_name

    return next_event if next_event else "No valid upcoming events found"

def rec_user_occasion(products, recommended_list):
    up_comming_event = get_nearest_upcoming_event()
    products_event = [product for product in products if up_comming_event in product.get('flower_details',{}).get('occasion',[])]
    #print(products_event)
    
    products_event = [product for product in products_event if product.get('product_id','') not in recommended_list and _is_sellable(product.get('product_id',''))]
    #print(products_event)
    
    if products_event:
        recommended_list.add(products_event[0].get('product_id',''))
        return {
            'flag' : 'occasion',
            'product_id' : products_event[0].get('product_id',''),
            'color' : '',
            'event' : up_comming_event
        }, recommended_list
    else:
        return {
            'flag' : 'occasion',
            'product_id' : '',
            'color' : '',
            'event' : ''
        }, recommended_list

############# Recommendation Cart

_pair_regex = re.compile(r"^([A-Za-z]+)(\d+)$")

def get_products_from_user_carts(user_id, carts):
    cart_item = next((c for c in carts if c["user_id"] == user_id), None)
    return {p["product_id"] for p in cart_item["products"]} if cart_item else set()

def _natural_key(pid: ProductID) -> Tuple[str, int]:
    m = _pair_regex.match(pid)
    if not m:
        return (pid, 0)
    prefix, num = m.groups()
    return (prefix, int(num))

def pair_normalize(product_id_1: ProductID, product_id_2: ProductID) -> str:
    a, b = sorted((product_id_1, product_id_2), key=_natural_key)
    return f"{a} - {b}"

def create_pair(
    product_list: Sequence[ProductID],
    pair_count_place_holder: Optional[MutableMapping[str, int]] = None, # MutableMapping -> Dictionary object type like Dictionary, Counter, etc
) -> Dict[str, int]:
    pair_count = pair_count_place_holder or Counter()
    unique_ids = tuple(set(product_list))
    for a, b in combinations(unique_ids, 2):
        key = pair_normalize(a, b)
        pair_count[key] = pair_count.get(key, 0) + 1
    return dict(pair_count)

def product_pair_count(order_items: Iterable[Mapping]) -> Dict[str, int]:
    counts = Counter()
    for order in order_items:
        product_ids = {item["product_id"] for item in order.get("products", [])}
        if not product_ids or len(product_ids) < 2:
            continue
        #print(product_ids)
        counts = create_pair(list(product_ids), counts)
    return dict(counts)

def rec_user_cross_selling(user_id, order_items, carts, recommended_list ):
    pair_count_db = product_pair_count(order_items)
    #print(pair_count_db)

    user_products_id: Set[ProductID] = set(get_products_from_user_carts(user_id, carts))
    #print(user_products_id)

    user_pairs = set(create_pair(list(user_products_id)).keys())
    #print(user_pairs)
    
    # Maybe use for further filtering
    # user_products_type = extract_type(user_products_id, products, ["flower", "vase"])
    # user_products_type_count, _, _ = user_data_counter(user_products_type) 

    candidates = []
    if user_products_id:
        for pair_key, freq in pair_count_db.items():
            p1, p2 = pair_key.replace(" ", "").split("-")
            # consider pairs that involve the user's cart items
            if (p1 in user_products_id) ^ (p2 in user_products_id) or (
                p1 in user_products_id and p2 in user_products_id
            ):
                # skip if the user already has the whole pair present
                if pair_key in user_pairs:
                    continue
                candidates.append((pair_key, freq))

    #print(candidates)

    if not candidates:
        return {
            'flag' : 'cross_selling',
            'product_id': '',
            'color' : '',
            'event': ''
        }, recommended_list
    
    candidates.sort(
        key=lambda kv: (-kv[1], tuple(map(_natural_key, kv[0].split(" - "))))
    )

    for pair_key, _freq in candidates:
        p1, p2 = pair_key.split(" - ")
        if p1 in user_products_id and p2 not in user_products_id:
            if p2 in recommended_list or not _is_sellable(p2):
                continue
            recommended_list.add(p2)
            return {
                'flag' : 'cross_selling',
                "product_id": p2,
                'color' : '',
                'event': ''
            }, recommended_list
        if p2 in user_products_id and p1 not in user_products_id:
            if p1 in recommended_list or not _is_sellable(p1):
                continue
            recommended_list.add(p1)
            return {
                'flag' : 'cross_selling',
                "product_id": p1,
                'color' : '',
                'event': ''
                }, recommended_list

    return {
        'flag' : 'cross_selling',
        'product_id': '',
        'color' : '',
        'event': ''
    }, recommended_list

############# Recommendation Best Selling
from collections import defaultdict

def rec_user_best_selling(user_id, order_items, carts, recommended_list):
    """
    Returns up to 3 best-selling product_ids.
    - First pass: obey filters (not in cart, not in recommended_list, sellable).
    - Backfill: ignore cart/recommended filters, but still require sellable.
    - Always returns at least 3 items if possible.
    """
    user_products_id = set(get_products_from_user_carts(user_id, carts))

    # 1) Build revenue per product
    revenue = defaultdict(float)
    for order in order_items:
        for product in order.get('products', []):
            pid = product.get('product_id', '')
            if not pid:
                continue
            opt = product.get('option') or {}
            qty = product.get('quantity', 0) or 0

            # prefer option price if present, else product price
            price = float(opt.get('price', product.get('price', 0.0) or 0.0))
            if qty:
                revenue[pid] += price * qty

    if not revenue:
        return {'flag': 'best', 'product_ids': [], 'color': '', 'event': ''}, recommended_list

    # 2) Sort products by revenue desc
    sorted_pids = [pid for pid, _rev in sorted(revenue.items(), key=lambda kv: kv[1], reverse=True)]

    picks = []

    # 3) Primary pass: apply filters
    for pid in sorted_pids:
        if len(picks) == 3:
            break
        if pid in user_products_id:
            continue
        if pid in recommended_list:
            continue
        if not _is_sellable(pid):
            continue
        picks.append(pid)

    # 4) Backfill to ensure 3 (ignore cart/recommended filters, still require sellable)
    if len(picks) < 3:
        for pid in sorted_pids:
            if len(picks) == 3:
                break
            if pid in picks:
                continue
            if not _is_sellable(pid):
                continue
            picks.append(pid)

    # Update recommended_list with everything we return
    recommended_list.update(picks)

    return {'flag': 'best', 'product_ids': picks[:3], 'color': '', 'event': ''}, recommended_list


############# Recommendation wrapper
def rec_user(user_id, top_k=3):
    global recommended_list
    recommended_list = set()  # reset per request/user

    rec_his, recommended_list  = rec_user_his(user_id, orders, order_items, products, top_k,recommended_list)
    rec_cross, recommended_list = rec_user_cross_selling(user_id, order_items, carts,recommended_list)
    rec_occ, recommended_list   = rec_user_occasion(products, recommended_list)
    rec_best, recommended_list  = rec_user_best_selling(user_id, order_items, carts,recommended_list)
    return rec_his, rec_cross, rec_occ, rec_best

def rec_user_converter(user_id, top_k = 3):
    rec_his, rec_cross, rec_occ, rec_best = rec_user(user_id, top_k)
    
    #print(rec_best.get('product_ids', []))
    
    if rec_his.get('product_id', '') == '' and rec_cross.get('product_id','') == '':
        rec_his['product_id'] = rec_best.get('product_ids',[])[1] 
        rec_cross['product_id'] = rec_best.get('product_ids',[])[2]
        
    if rec_his.get('product_id','') == '':
        rec_his['product_id'] = rec_best.get('product_ids',[])[1]
        
    if rec_cross.get('product_id','') == '':
        rec_cross['product_id'] = rec_best.get('product_ids',[])[1]
        
    rec_best['product_id'] = rec_best.get('product_ids',[])[0]
    
    print(rec_best['product_ids'])
        
    return rec_his, rec_cross, rec_occ, rec_best

# Data_loader
def load_data(data_path='data.json'):
    global orders, order_items, products, carts, _PRODUCT_BY_ID
    orders, order_items, products, carts = init_data(data_path)
    _PRODUCT_BY_ID = {p["product_id"]: p for p in products}

# call once at module import (keeps current behavior)
load_data()

# also guard the demo print so it doesn't run on import
if __name__ == "__main__":
    print(rec_user_converter(1))
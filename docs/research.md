
Search bar use this GET request to search for product:
- url: https://ultimate-dot-acp-magento.appspot.com/?q=rp4&s=www.northcountryfire.com&cdn_cache_key=1620385114&v=2021.05.05&store_id=14034773&UUID=34efc3a6-91d4-4403-99fa-5633d6e9a5bd&callback=acp_magento_acp_new2
- Required:
  - `q`: query
  - `UUID`: seem to be NCF UUID for this platform
- Optional:
  - `&callback=acp_magento_acp_new2`: display shortform JSON


Browser JS `localStorage` saving some useful info, these commands are typed into browser console:

- Seems to be all of NCF 'collections':
  - `console.log(typeof localStorage.ISP_POP_CATEGORIES)` -> `'string'`
  - `console.log(JSON.parse(localStorage.ISP_POP_CATEGORIES).length)` -> `3067`
  - Command:

    ```js
    let catObj = JSON.parse(localStorage.ISP_POP_CATEGORIES)
    let lastCatObj = catObj[3066]
    console.log(JSON.stringify(lastCatObj))
    ```

    Result:

    ```js
    {
        "d": "",
        "p_id": "1",
        "l": "Majestic Twilight II Modern Indoor/Outdoor See-Through Gas Fireplace | TWILIGHT-II-MDC |",
        "u": "/collections/majestic-twilight-ii-modern-indoor-outdoor-see-through-gas-fireplace-twilight-ii-mdc",
        "t": "https://magento.instantsearchplus.com/images/missing.gif",
        "id": "162013052993",
        "type": "m",
        "label": "Majestic Twilight II Modern Indoor/Outdoor See-Through Gas Fireplace | TWILIGHT-II-MDC |",
        "label_lower": "majestic twilight ii modern indoor/outdoor see through gas fireplace | twilight ii mdc |",
        "product_url": "https://www.northcountryfire.com/collections/majestic-twilig…ern-indoor-outdoor-see-through-gas-fireplace-twilight-ii-mdc",
        "thumbs_url": "https://magento.instantsearchplus.com/images/missing.gif",
        "category": true
    }
    ```


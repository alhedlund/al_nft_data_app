mkdir -p ~/.streamlit/

echo "[theme]
primaryColor = ‘#d33682’
backgroundColor = ‘#002b36’
secondaryBackgroundColor = ‘#586e75’
font = ‘monospace’
[server]
headless = true
port = $PORT
enableCORS = false
" > ~/.streamlit/config.toml
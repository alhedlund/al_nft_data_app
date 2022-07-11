mkdir -p ~/.streamlit/

echo "\
[general]\n\
email = \"your-email@domain.com\"\n\
" > ~/.streamlit/credentials.toml

echo "[theme]
primaryColor=’#d33682’
backgroundColor=’#002b36’
secondaryBackgroundColor=’#586e75’
font = ‘monospace’
base=‘dark’
[server]
headless = true
port = $PORT
enableCORS = false
" > ~/.streamlit/config.toml
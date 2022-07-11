mkdir -p ~/.streamlit/

echo "\
[server]\n\
headless = true\n\
port = $PORT\n\
enableCORS = false\n\
\n\
" > ~/.streamlit/config.toml

echo "\
[general]\n\
email = \"your-email@domain.com\"\n\
" > ~/.streamlit/credentials.toml

echo '[theme]
base="dark"
primaryColor="#d33682"
backgroundColor="#002b36"
secondaryBackgroundColor="#586e75"
font="monospace"
'
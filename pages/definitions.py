import streamlit as st


def app():
    st.title("Metric Definitions")

    definition_text = """
        Floor: 
        Our floor is slightly different than what you might expect because it's 
        based on sales instead of listings because we pull data from the blockchain where 
        OpenSea listings aren't available. Because of this, floors will appear 
        to lag behind OpenSea listings -- however, we think this is still an 
        accurate representation of a floor price that is based on liquidity.
        
        Circulating Supply: 
        This is the current circulating supply we've calculated for a 
        given collection by summing mints and subtracting any NFTs that have been burned.
        
        Average: 
        The average price over the given period.
        
        Volume: 
        The total volume over the given period.
        
        Sales: 
        The total number of sales over the given period.
        
        Market Cap: 
        The floor price multiplied by circulating supply.
    """

    st.write(definition_text)

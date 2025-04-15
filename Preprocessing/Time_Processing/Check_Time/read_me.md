### Intro
These scripts were used to figure out the time metadata for the various TRENDY models. 

1. Check time stamps no decode
    - This extracts the meta data for each file without decoding the time axis and stores each in a separate CSV at the output directory.
    - As time is not decoded, the start and end dates cannot be discovered.

2. Check time stamps attempt decode
    - This does the same as above but also tries to decode all of the various time axes.
    - It was not successful in decoding all of them, some proved tricky. 
    - But by doing so the start dates of most of the data could be discovered and the number of timesteps was used as the key indicator. Once it was clear for example that 1968 timesteps meant monthly data starting 1860-01-01 it was easy to figure out the rest.

3. Plot timestamps
    - This plots each of CSVs found created by the above scripts.
    - Colours are nicer but if theres more than 20 unique values they repeat.

4. Plot timestamps strong contrast
    - This plots the same as above but without repeating colours (but they are a bit sore on the eyes).
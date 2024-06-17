import logging
from quixstreams import Application
from uuid import uuid4
from datetime import timedelta
import pygsheets

def initializer_fn(msg): # get a single msg from the very start of the hour and has to turn it into a summary
    temperature = msg['current']['temperature_2m']

    return {
        "open": temperature,
        "high": temperature,
        "low": temperature,
        "close": temperature
     }

def reducer_fn(summary, msg): # take a the summary so far and the new message then return a new summary
    # take the temp out of the new msg first
    temperature = msg['current']['temperature_2m']
    
    return {
        "open": summary['open'], # the open value is always the first ever value
        "high": max(summary['high'], temperature) , # the maximum of what we've seen
        "low": min(summary['low'], temperature),
        "close": temperature, # the last value we know
     }


def main():
    app = Application(
        broker_address = "localhost:9092",
        loglevel = "DEBUG",
        consumer_group = str(uuid4()),
        auto_offset_reset = "earliest",
     )

    input_topic = app.topic("weather_data_demo")

    sdf = app.dataframe(input_topic)
    
    #sdf = sdf.group_into_hourly_batches()
    sdf = sdf.tumbling_window(duration_ms=timedelta(hours=1)) # an hour long block of data

    #sdf = sdf.summarize_that_hour()
    sdf = sdf.reduce(
        initializer = initializer_fn,
        reducer = reducer_fn
    ) # reduce hours worth of data to a single summary row
    
    # once you're done with the window processing calls, you need to finalize the window to get back into the regular data frame api
    sdf = sdf.final()

    # .update() is used to apply a function to each message
    # for every frame, log out the message
    #sdf = sdf.update(lambda msg: logging.debug("Got: %s", msg))
   
   #sdf = sdf.send_to_gg_sheets()
    
    # what the application will do now is go back and reprocess all the hours, we would get a series of hourly summaries that tell us the timestamp at the start and end of the hour and the value of our summary object. It would just sit there, waiting for more data to come in until the end of the hour then will finish off the processing and start waiting for the next hour worth of data.
    app.run(sdf)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    #main()
    
    google_api = pygsheets.authorize()
    workspace = google_api.open("Weather Data")
    sheet = workspace[0]
    
    # write to specific cells
    sheet.update_values(
        "A1",
        [
            ["Hello", "Spreadsheet"],
            ["I am row", "two"],
        ],
     )
    print(sheet)

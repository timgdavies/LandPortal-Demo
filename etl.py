from dataflows import Flow, load, dump_to_path, dump_to_zip, printer, add_metadata
from dataflows import sort_rows, filter_rows, find_replace, delete_fields, set_type, validate, unpivot
import zipfile
import requests
from io import BytesIO

def etl():
    # Download the latest data and extract
    file = requests.get("https://gain.nd.edu/assets/437409/resources.zip")
    zipped = zipfile.ZipFile(BytesIO(file.content))
    zipped.extractall('input/')

    unpivoting_fields = [
    { 'name': '([0-9]{4})', 'keys': {'year': r'\1'} }]

    # A newly created column header would be 'year' with type 'year':
    extra_keys = [ {'name': 'year', 'type': 'year'} ]
    # And values will be placed in the 'value' column with type 'string':
    extra_value = {'name': 'value', 'type': 'string'}

    # Convert to a common format
    flow = Flow(
        # Load inputs
        load('input/resources/gain/gain.csv', format='csv', ),
        # Process them (if necessary)

        unpivot(unpivoting_fields, extra_keys, extra_value),
        
        # Save the results
        add_metadata(name='gain_index', title='Gain Index'),

        # Output
        dump_to_path(out_path='output'),
        printer()

    )
    flow.process()


if __name__ == '__main__':
    etl()

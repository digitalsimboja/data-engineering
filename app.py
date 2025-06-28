from chalice import Chalice

app = Chalice(app_name='data-segmentation-api')


@app.route('/')
def index():
    return {'hello': 'world'}

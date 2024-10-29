import os
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import (
    PageNumberPaginator,
    SinglePagePaginator,
)

TOKEN = os.getenv("SOURCES__TMDB__BEARER_TOKEN")


@dlt.source(name="api_tmdb")
def source():
    client = RESTClient(
        base_url="https://api.themoviedb.org/3/",
        auth=BearerTokenAuth(token=dlt.secrets.value),
        data_selector="results",
        paginator=PageNumberPaginator(
            base_page=1, page_param="page", total_path="total_pages"
        ),
    )

    @dlt.resource(name="movie", parallelized=True)
    def movie():
        for page in client.paginate(path="movie/top_rated"):
            yield page

    @dlt.resource(name="tv", parallelized=True)
    def tv():
        for page in client.paginate(path="tv/top_rated"):
            yield page

    @dlt.transformer(name="movie_watch_providers", data_from=movie, parallelized=True)
    def movie_watch_providers(data):
        client.paginator = SinglePagePaginator()
        client.data_selector = "id|results.US|results.BR"
        for movie in data:
            for page in client.paginate(path=f"movie/{movie['id']}/watch/providers"):
                yield page

    @dlt.transformer(name="tv_watch_providers", data_from=tv, parallelized=True)
    def tv_watch_providers(data):
        client.paginator = SinglePagePaginator()
        client.data_selector = "id|results.US|results.BR"
        for tv in data:
            for page in client.paginate(path=f"tv/{tv['id']}/watch/providers"):
                yield page

    return [movie(), tv(), movie_watch_providers(), tv_watch_providers()]

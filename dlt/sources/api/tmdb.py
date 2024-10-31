import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import (
    PageNumberPaginator,
    SinglePagePaginator,
)


def watch_providers_selector(data, regions=["US", "BR"]):
    """Selects watch provider info for specified regions."""
    return {
        key: value
        for key, value in {
            "id": data.get("id"),
            "US": data.get("results", {}).get("US"),
            "BR": data.get("results", {}).get("BR"),
        }.items()
        if value is not None
    }


@dlt.source(name="api_tmdb")
def source():
    """Defines the TMDb API source with resources and transformers."""
    client = RESTClient(
        base_url="https://api.themoviedb.org/3/",
        auth=BearerTokenAuth(token=dlt.secrets["sources.api.tmdb.token"]),
    )

    @dlt.resource(
        name="movie",
        parallelized=True,
    )
    def movie():
        """Fetches top-rated movies from the TMDb API."""
        for page in client.paginate(
            path="movie/top_rated",
            paginator=PageNumberPaginator(
                base_page=1, 
                page_param="page", 
                total_path="total_pages"
            ),
            data_selector="results",
        ):
            yield page

    @dlt.resource(
        name="tv",
        parallelized=True,
    )
    def tv():
        """Fetches top-rated TV shows from the TMDb API."""
        for page in client.paginate(
            path="tv/top_rated",
            paginator=PageNumberPaginator(
                base_page=1, 
                page_param="page", 
                total_path="total_pages"
            ),
            data_selector="results",
        ):
            yield page

    @dlt.resource(
        name="movie_genres",
        parallelized=True,
    )
    def movie_genres():
        """Fetches the list of movie genres from the TMDb API."""
        for page in client.paginate(
            path="genre/movie/list",
            paginator=SinglePagePaginator(),
            data_selector="genres",
        ):
            yield page

    @dlt.resource(
        name="tv_genres",
        parallelized=True,
    )
    def tv_genres():
        """Fetches the list of TV genres from the TMDb API."""
        for page in client.paginate(
            path="genre/tv/list",
            paginator=SinglePagePaginator(),
            data_selector="genres",
        ):
            yield page

    @dlt.transformer(
        name="movie_watch_providers_availability", 
        data_from=movie, 
        parallelized=True
    )
    def movie_watch_providers_availability(data):
        """Fetches watch provider availability for each movie."""
        for movie in data:
            for page in client.paginate(
                path=f"movie/{movie['id']}/watch/providers",
                paginator=SinglePagePaginator(),
                data_selector="$",
            ):
                yield page

    @dlt.transformer(
        name="tv_watch_providers_availability", 
        data_from=tv, 
        parallelized=True
    )
    def tv_watch_providers_availability(data):
        """Fetches watch provider availability for each TV show."""
        for tv in data:
            for page in client.paginate(
                path=f"tv/{tv['id']}/watch/providers",
                paginator=SinglePagePaginator(),
                data_selector="$",
            ):
                yield page

    return [
        movie(),
        tv(),
        movie_genres(),
        tv_genres(),
        movie_watch_providers_availability().add_map(
            lambda data: watch_providers_selector(data)
        ),
        tv_watch_providers_availability().add_map(
            lambda data: watch_providers_selector(data)
        ),
    ]

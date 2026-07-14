from series_archiver import SeriesArchiver


def test_plan_to_watch_mal_status_classifies_as_plan_to_watch():
    archiver = SeriesArchiver()
    group_data = {
        "files": [{"type": "episode", "episode_watched": False}],
        "episodes_found": 3,
        "watch_status": {
            "watched_episodes": 0,
            "partially_watched_episodes": 0,
        },
        "myanimelist_watch_status": {
            "my_status": "Plan to Watch",
        },
    }

    assert archiver._get_watch_status_classification(group_data) == "plan_to_watch"

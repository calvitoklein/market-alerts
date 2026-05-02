"""
Direct Twitter/X GraphQL API client using session cookies.
No dependency on twscrape - avoids xclid.py breakage.

Configure once:
    python setup_cookies.py --auth_token XXX --ct0 YYY

Then use:
    from twitter_api import TwitterAPI
    api = TwitterAPI.from_env()
    tweets = api.user_tweets("Pentoshi", limit=20)
"""

import os, json, time
from datetime import datetime, timezone
from typing import Iterator

import requests

DIR = os.path.dirname(os.path.abspath(__file__))
COOKIES_FILE = os.path.join(DIR, "twitter_cookies.json")

BEARER = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

GQL = "https://twitter.com/i/api/graphql"
USER_BY_SCREEN_NAME_ID = "G3KGOASz96M-Qu0nwmGXNg"
USER_TWEETS_ID         = "V7H0Ap3_Hh2FyS75OCDO3Q"

USER_BY_SCREEN_NAME_FEATURES = json.dumps({
    "hidden_profile_likes_enabled": True,
    "hidden_profile_subscriptions_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "highlights_tweets_tab_ui_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "subscriptions_verification_info_is_identity_verified_enabled": True,
    "subscriptions_verification_info_verified_since_enabled": True,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_twitter_article_notes_tab_enabled": False,
})

USER_TWEETS_FEATURES = json.dumps({
    "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
})


class TwitterAPI:
    def __init__(self, auth_token: str, ct0: str):
        self.session = requests.Session()
        self.session.cookies.update({"auth_token": auth_token, "ct0": ct0})
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Authorization": f"Bearer {BEARER}",
            "x-csrf-token": ct0,
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "en",
            "x-twitter-active-user": "yes",
            "content-type": "application/json",
        })
        self._user_id_cache: dict[str, str] = {}

    @classmethod
    def from_file(cls, path: str = COOKIES_FILE) -> "TwitterAPI":
        with open(path) as f:
            data = json.load(f)
        return cls(data["auth_token"], data["ct0"])

    @classmethod
    def from_env(cls) -> "TwitterAPI | None":
        if os.path.exists(COOKIES_FILE):
            try:
                return cls.from_file()
            except Exception:
                pass
        return None

    def save_cookies(self, auth_token: str, ct0: str, path: str = COOKIES_FILE):
        with open(path, "w") as f:
            json.dump({"auth_token": auth_token, "ct0": ct0}, f)

    def _get(self, url: str, params: dict) -> dict | None:
        try:
            r = self.session.get(url, params=params, timeout=15)
            if r.status_code == 200 and r.content:
                return r.json()
        except Exception:
            pass
        return None

    def get_user_id(self, handle: str) -> str | None:
        if handle in self._user_id_cache:
            return self._user_id_cache[handle]
        variables = json.dumps({"screen_name": handle, "withSafetyModeUserFields": True})
        data = self._get(
            f"{GQL}/{USER_BY_SCREEN_NAME_ID}/UserByScreenName",
            {"variables": variables, "features": USER_BY_SCREEN_NAME_FEATURES},
        )
        try:
            uid = data["data"]["user"]["result"]["id"]
            # Decode base64 "User:NNNN" → "NNNN"
            import base64
            decoded = base64.b64decode(uid).decode()
            numeric_id = decoded.split(":")[-1]
            self._user_id_cache[handle] = numeric_id
            return numeric_id
        except Exception:
            return None

    def _extract_tweets_from_timeline(self, timeline: dict) -> list[dict]:
        tweets = []
        try:
            instructions = (
                timeline.get("data", {})
                        .get("user", {})
                        .get("result", {})
                        .get("timeline_v2", {})
                        .get("timeline", {})
                        .get("instructions", [])
            )
            for instr in instructions:
                for entry in instr.get("entries", []):
                    content = entry.get("content", {})
                    item_content = content.get("itemContent", {})
                    if item_content.get("itemType") != "TimelineTweet":
                        continue
                    tweet_result = item_content.get("tweet_results", {}).get("result", {})
                    tweet_legacy = tweet_result.get("legacy", {})
                    if not tweet_legacy:
                        # might be nested under core
                        tweet_legacy = tweet_result.get("tweet", {}).get("legacy", {})
                    if not tweet_legacy:
                        continue

                    text = tweet_legacy.get("full_text") or tweet_legacy.get("text") or ""
                    created_at = tweet_legacy.get("created_at", "")
                    try:
                        ts = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y").replace(tzinfo=timezone.utc)
                    except Exception:
                        ts = None

                    media_urls = []
                    for m in tweet_legacy.get("entities", {}).get("media", []):
                        if m.get("type") == "photo":
                            media_urls.append(m.get("media_url_https", ""))

                    tweets.append({
                        "title":    text,
                        "summary":  text,
                        "_ts":      ts,
                        "_source":  "twitter_api",
                        "_images":  media_urls,
                    })
        except Exception:
            pass
        return tweets

    def user_tweets(self, handle: str, limit: int = 20) -> list[dict]:
        uid = self.get_user_id(handle)
        if not uid:
            return []
        variables = json.dumps({
            "userId": uid,
            "count": min(limit, 40),
            "includePromotedContent": False,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            "withV2Timeline": True,
        })
        data = self._get(
            f"{GQL}/{USER_TWEETS_ID}/UserTweets",
            {"variables": variables, "features": USER_TWEETS_FEATURES},
        )
        if not data:
            return []
        return self._extract_tweets_from_timeline(data)[:limit]


def save_cookies(auth_token: str, ct0: str):
    with open(COOKIES_FILE, "w") as f:
        json.dump({"auth_token": auth_token, "ct0": ct0}, f)
    print(f"Cookies guardadas en {COOKIES_FILE}")


def is_configured() -> bool:
    return os.path.exists(COOKIES_FILE)


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3:
        save_cookies(sys.argv[1], sys.argv[2])
        print("Guardado. Probando...")
        api = TwitterAPI(sys.argv[1], sys.argv[2])
        tweets = api.user_tweets("Pentoshi", limit=3)
        if tweets:
            for tw in tweets:
                print(f"  [{tw['_ts']}] {tw['title'][:80]}")
            print(f"\nOK: {len(tweets)} tweets obtenidos.")
        else:
            print("ERROR: no se obtuvieron tweets. Verifica las cookies.")
    else:
        print("Uso: python twitter_api.py <auth_token> <ct0>")

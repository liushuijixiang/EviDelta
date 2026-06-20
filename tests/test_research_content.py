import json

import httpx

from feishu_agent_bot.llm.schemas import SearchResult
from feishu_agent_bot.research.deduplication import (
    content_hash,
    deduplicate_search_results,
)
from feishu_agent_bot.research.parser import ContentExtractor
from feishu_agent_bot.research.search import DDGSSearchProvider, SerperSearchProvider


def test_search_result_url_deduplication():
    rows = [
        SearchResult(
            title="A",
            url="https://example.com/a#one",
            query="q",
            rank=1,
        ),
        SearchResult(
            title="A2",
            url="https://EXAMPLE.com:443/a#two",
            query="q",
            rank=2,
        ),
    ]
    assert len(deduplicate_search_results(rows)) == 1


def test_content_hash_ignores_whitespace():
    assert content_hash("hello   world") == content_hash("hello world")


def test_html_content_extraction_removes_navigation():
    html = b"""
    <html><head><title>Example report</title><style>bad</style></head>
    <body><nav>navigation must disappear completely</nav>
    <main><h1>Market report heading</h1>
    <p>This is the useful main paragraph with enough detail to retain.</p>
    <script>secret script content</script></main><footer>footer text</footer>
    </body></html>
    """
    page = ContentExtractor().extract(html, "https://example.com/report")
    assert page.title == "Example report"
    assert "useful main paragraph" in page.text
    assert "navigation" not in page.text
    assert "secret script" not in page.text


def test_html_content_extraction_prefers_http_gbk_over_wrong_utf8_meta():
    html = """
    <html><head><meta charset="utf-8"><title>A股-研报详情</title></head>
    <body><main><p>这是使用 GBK 编码的研报正文，应该被正确解码和提取。</p></main></body>
    </html>
    """.encode("gbk")

    page = ContentExtractor().extract(
        html,
        "https://stock.finance.sina.com.cn/report",
        "text/html; charset=gbk",
    )

    assert page.title == "A股-研报详情"
    assert "这是使用 GBK 编码的研报正文" in page.text
    assert "�" not in page.title
    assert "�" not in page.text


def test_html_content_extraction_falls_back_to_gb18030_without_charset():
    html = """
    <html><head><title>新能源汽车行业报告</title></head>
    <body><main><p>报告包含中文扩展字符和完整的市场分析正文。</p></main></body>
    </html>
    """.encode("gb18030")

    page = ContentExtractor().extract(
        html, "https://example.com/report", "text/html"
    )

    assert page.title == "新能源汽车行业报告"
    assert "完整的市场分析正文" in page.text


def test_search_provider_filters_obvious_spam(monkeypatch):
    class FakeDDGS:
        def text(self, *args, **kwargs):
            return [
                {
                    "title": "同城外围小姐上门服务",
                    "href": "https://spam.example/yp999",
                    "body": "垃圾内容",
                },
                {
                    "title": "新能源汽车充电设施商业模式分析",
                    "href": "https://example.com/report",
                    "body": "行业研究",
                },
            ]

    monkeypatch.setattr("ddgs.DDGS", FakeDDGS)
    rows = DDGSSearchProvider().search("新能源汽车充电设备", 2)
    assert [row.url for row in rows] == ["https://example.com/report"]


def test_serper_search_provider_maps_organic_results():
    request_body = {}
    request_headers = {}

    def handler(request):
        request_body.update(json.loads(request.content))
        request_headers.update(request.headers)
        return httpx.Response(
            200,
            request=request,
            json={
                "organic": [
                    {
                        "title": "行业报告",
                        "link": "https://example.com/report",
                        "snippet": "充电设备竞品分析",
                    },
                    {
                        "title": "同城外围小姐上门服务",
                        "link": "https://spam.example/yp999",
                        "snippet": "垃圾内容",
                    },
                ]
            },
        )

    provider = SerperSearchProvider(
        api_key="search-key",
        country="cn",
        locale="zh-cn",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    rows = provider.search("新能源汽车充电设备", 2)
    assert request_headers["x-api-key"] == "search-key"
    assert request_body == {
        "q": "新能源汽车充电设备",
        "num": 2,
        "gl": "cn",
        "hl": "zh-cn",
    }
    assert [row.url for row in rows] == ["https://example.com/report"]


def test_serper_search_provider_keeps_file_results():
    def handler(request):
        return httpx.Response(
            200,
            request=request,
            json={
                "organic": [
                    {
                        "title": "PDF 行业报告",
                        "link": "https://example.com/report.pdf",
                        "snippet": "白皮书",
                    },
                ]
            },
        )

    provider = SerperSearchProvider(
        api_key="search-key",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    rows = provider.search("行业报告 filetype:pdf", 1)

    assert rows[0].url == "https://example.com/report.pdf"
    assert rows[0].likely_asset_type == "pdf"
    assert rows[0].detected_extension == ".pdf"

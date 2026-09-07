import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell
async def _():
    from playwright.async_api import async_playwright

    url = "https://mainichi.jp/flash/"
    article_selector = "#article-list > ul > li:nth-child(22)"
    more_selector = "div.main-contents span.link-more"

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1920, "height": 1080})
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            await page.locator(article_selector).wait_for(state="attached", timeout=60_000)
            more_button = page.locator(more_selector)
            await more_button.wait_for(state="visible", timeout=60_000)
            await more_button.scroll_into_view_if_needed()

            button_text = await more_button.inner_text()
            result = {
                "url": page.url,
                "article_selector": article_selector,
                "more_button_text": button_text,
            }
        finally:
            await browser.close()

    return (result,)


@app.cell
def _(result):
    return marimo.md(f"```python\n{result}\n```")


if __name__ == "__main__":
    app.run()

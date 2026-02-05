# Case01: Crawl a website and save the result to a file
import asyncio
import os
import re
from crawl4ai import CrawlerRunConfig, AsyncWebCrawler
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from util import url2fname
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
from table_unspanner import TableUnspanner
from loguru import logger
load_dotenv()

config = CrawlerRunConfig(
    # Content thresholds
    # word_count_threshold=10,        # Minimum words per block
    # remove_overlay_elements=True,
    remove_overlay_elements=False,
    scraping_strategy=LXMLWebScrapingStrategy(),  # Faster alternative to default BeautifulSoup
    # js_code=[
    #     "document.getElementById('check-in-box')?.click();",
    # ],
    # Exclude elements such as #header like <div id="header">...</div>
    # tepco-ep pc用のselectorは除外しsp(スマホ)用のselectorは残す
    excluded_selector = os.getenv("EXCLUDE_SELECTOR", "#header, .header, #footer, .footer"),
    # Tag exclusions
    excluded_tags=['form', 'header', 'breadcrumbs' , 'footer', 'nav'],
    process_iframes=True,
    # Link filtering
    exclude_external_links=False,    
    exclude_social_media_links=False,
    # Block entire domains
    exclude_domains=["adservice.google.com", "adtrackers.com", "spammynews.org"],    
    exclude_social_media_domains=["facebook.com", "x.com"],

    # Media filtering
    exclude_external_images=False,
)
import re
def _asahi_beer_fix_asterisk_only_in_markdown(markdown_text) -> str:
    """ Fix lines that contain only asterisks and spaces which may break markdown formatting.
    """
    fixed_text = re.sub(r'^\s{2}\* \n', '', markdown_text, flags=re.MULTILINE)
    return fixed_text
def _asahi_beer_fix_two_asterisks_in_markdown(markdown_text) -> str:
    fixed_text = re.sub(r'^\s{2}\*\s{5}\*', '    *', markdown_text, flags=re.MULTILINE)
    return fixed_text
def _asahi_beer_remove_asterisk_of_heading_links(markdown_text) -> str:
    """
    Docstring for _asahi_beer_remove_asterisk_of_heading_links
    [before]
        * ![合うお酒](https://...
    [after]
        ![合うお酒](https://...
    """
    fixed_text = re.sub(r'^\s{2}\* (!\[)', r'\n\1', markdown_text, flags=re.MULTILINE)
    return fixed_text

def _kewpie_fix_markdown_table_linebreaks(markdown_text):
    """Fix line breaks in markdown tables
    <br>タグなどで改行されてしまったテーブルのセルを結合する。
    例えば、以下のようなケースを修正する。
    [修正前]
        卵（Mサイズ） | 2個 | 100g  
        ---|---|---  
        A 顆粒和風だし | 小さじ  
        1/5 |   
        A 水 |  | 45ml  
        A うすくちしょうゆ | 小さじ  
        1/2 |   
    [修正後]
        卵（Mサイズ） | 2個 | 100g  
        ---|---|---  
        A 顆粒和風だし | 小さじ 1/5 |   
        A 水 |  | 45ml  
        A うすくちしょうゆ | 小さじ 1/2 |   
    """
    lines = markdown_text.split('\n')
    fixed_lines = []
    
    for i, line in enumerate(lines):
        # Check if this line is part of a table (contains |)
        if not line:
            continue
        if '|' in line:
            # Check if the next line is also part of the same table row
            # (doesn't start with |--- or blank)
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                if re.search(r'[^ ]  $', line) and not line.startswith('---'):
                    if '|' in next_line and  re.search(r'[^ ]   $', next_line) and not next_line.startswith('---') and not next_line.startswith('|') and not next_line.startswith('A |'):
                    
                        # Merge with next line
                        line = line.rstrip() + ' ' + next_line.strip()
                        lines[i + 1] = None  # Clear the next line
        
        if line:  # Only add non-empty lines
            fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def _kewpie_fix_table_markdown(md_text):
    """ Fix markdown tables from kewpie site where some rows have missing columns.
        材料セクションのテーブルの列数を揃える。"A"が列の先頭に存在するため、Craawl4AIのテーブル抽出ロジックが正しく列数を認識できないため。
    """
    def matches_ingredient_header(line):
        patterns = [r'^#+ .*材料（[0-9０-９]+人分）', 
                    r'^#+ .*つけあわせ'
                ]

        for pattern in patterns:
            if re.match(pattern, line):
                return True
        return False

    # Split into lines
    lines = md_text.split('\n')
    
    # Find table sections (lines with |)
    table_lines = [l for l in lines if '|' in l]
    print(f"Found {len(table_lines)} table lines.") 
    if not table_lines:
        return md_text
    
    ingredients_section = False
    max_cols = 0
    
    # Rebuild with consistent columns
    for line in lines:
        if matches_ingredient_header(line):
            cells = [c.strip() for c in line.split('|')]

            ingredients_section = True
            continue  # Skip this line
        if ingredients_section:
            if '|' in line:
                cells = [c.strip() for c in line.split('|')]
                if len(cells) > max_cols:
                    max_cols = len(cells)
            else:
                ingredients_section = False
            break  # Only need to determine max_cols once

    logger.debug(f"Determined max columns in ingredients table: {max_cols}")
        
    fixed_lines = []
    ingredients_section = False
    for line in lines:
        if matches_ingredient_header(line):
            ingredients_section = True
            fixed_lines.append(line)
            continue  # Skip this line
        
        if ingredients_section:
            if '|' in line:
                cells = [c.strip() for c in line.split('|')]
                # print(f"Original cells: {cells}")
                # Pad to max_cols + 2 (for leading/trailing |)
                # while len(cells) < max_cols + 2:
                logger.debug(f"Fixing line: {line} with {len(cells)} cells.")
                while len(cells) < max_cols + 1:
                    cells.append('<sp>')
                logger.debug(f"cells[0:-1]: {cells[0:-1]}") 
                # fixed_lines.append('| ' + ' | '.join(cells[1:-1]) + ' |')
                fixed_lines.append('| ' + ' | '.join(cells[0:-1]) + ' |')
            else:
                ingredients_section = False
                fixed_lines.append(line)
        else:
            fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def fix_multiline_table_cells(markdown_text: str) -> str:
    """ Fix multiline table cells by merging lines that are part of the same cell.
    Args:
        markdown_text: The markdown content as a string.
    Returns:
        The modified markdown content with multiline cells merged."""
    lines = markdown_text.split('\n')
    result_lines = []
    i = 0
    
    while i < len(lines):
        current_line = lines[i]
        
        # If this is a table row (contains |)
        if '|' in current_line and current_line.strip():
            # Collect all subsequent lines that don't start with | or contain |
            collected_lines = [current_line.rstrip()]
            j = i + 1
            
            while (j < len(lines) and 
                    lines[j].strip() and
                    not lines[j].lstrip().startswith('|') and
                    not lines[j].lstrip().startswith('-') and
                    not lines[j].lstrip().startswith('#') and
                    '|' not in lines[j]):
                collected_lines.append(lines[j].strip())
                j += 1
            
            # Join with <br> if we have multiple lines
            if len(collected_lines) > 1:
                merged_line = '<br>'.join(collected_lines)
                result_lines.append(merged_line)
            else:
                result_lines.append(current_line)
            
            i = j
        else:
            result_lines.append(current_line)
            i += 1
    
    return '\n'.join(result_lines)



def remove_javascript_void_zero(markdown_text: str) -> str:
    """ Remove '(javascript:void(0);)' or '(javascript:void(0))' from the content.
    idやclass属性が指定されていないaタグなどに取得すべき文字列が含まれることがあるため、
    excluded_selectorに指定できない。このためこの関数を用意し除する。
    aタグなどに含まれるケースについては、images/javascript_void_zero.png を参照。

    Args:
        content: The markdown content as a string.
    Returns:
        The modified markdown content without '(javascript:void(0);)' or '(javascript:void(0))' ."""
    return re.sub(r'\(javascript:void\\\(0\\\);?\)', '', markdown_text)

def adjust_numbered_lists(markdown_text: str) -> str:
    """
    Convert numbered lists to proper markdown format with dots.
    Handles:
    - Lines starting with numbers (1玉ねぎ → 1. 玉ねぎ)
    - Preserves existing proper format (1. already formatted)
    - Ignores numbers in middle of lines
    """
    def matched_unformatted_number(line: str) -> bool:
        """ 
        以下のようなパターンはマッチの除外する。
        1の...　-> 1.の [NG]
        1か(1から) -> 1.か [NG]
        [格助詞] 1の, 1が, 1を, 1に, 1へ, 1と, 1から, 1より, 1まで 
        [副助詞] 1も, 1は, 1だけ, 1しか, 1こそ, 1でも, 1ずつ
        [接続助詞] 1で, 1なら
        [接尾辞的な語] 1人, 1個, 1本, 1枚, 1羽, 1台, 1回, 1番, 1日, 1年, 1ページ, 1グラム, 1cc, 1g, 1kg
            """
        pattern = r'^(\d+)([^\.\s/\+\-*%\^<>\[\]\(\)のがをにへとかよまもはだしこでずなつ個本枚羽台回番日年度ペキグcg])'
        
        matched = re.match(pattern, line)
        if matched:
            return matched
        return None

    lines = markdown_text.split('\n')
    adjusted_lines = []
    
    for line in lines:
        stripped_line = line.lstrip()
        leading_space = line[:len(line) - len(stripped_line)]
        
        # Pattern: starts with digit(s), no dot/space after, followed by content
        matched = matched_unformatted_number(stripped_line)
        if matched:
            logger.debug(f"Line: {line} Matched: {matched}") 
            number = matched.group(1)
            rest = stripped_line[len(number):]
            adjusted_line = f"\n{leading_space}{number}. {rest}"
            adjusted_lines.append(adjusted_line)
        else:
            adjusted_lines.append(line)
    return '\n'.join(adjusted_lines)

def adjust_markdown(markdown: str) -> str:

    list_of_functions = [
        # _kewpie_fix_markdown_table_linebreaks,
        # _kewpie_fix_table_markdown,
        # remove_javascript_void_zero,
        # adjust_numbered_lists,
        _asahi_beer_fix_asterisk_only_in_markdown,
        _asahi_beer_fix_two_asterisks_in_markdown,
        _asahi_beer_remove_asterisk_of_heading_links,
    ]
    for func in list_of_functions:
        markdown = func(markdown)
    return markdown

async def crawl(input_file='urls.txt', output_dir='output_crawled'):
    """Crawl the URLs from the input file and save the results to the output directory."""
    urls = []

    with open(input_file, 'r') as input_file:
        for line in input_file:
            url = line.strip()
            if url and len(url) > 0 and not url.startswith('#'):
                urls.append(url)



    if not Path(output_dir).exists():
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    # AsyncWebCrawlerは、Single browser instanceとして動作するため、複数のインスタスを生成すると
    # リソース逼迫によりハングアップするため、urlsのループ内で生成しないこと。
    # Crawl4AIの現時点(2025-12)の最新 v 0.7.8では、大量のURLをクロールする場合に、リソースのリークが発生していると思われ、
    # すべてのURLをクロールし終えた後に正常終了せずにハングアップすることがある。
    async with AsyncWebCrawler() as crawler:
        for i, url in enumerate(urls):
            try:
                result = await crawler.arun(
                    url=url,
                    bypass_cache=True,
                    config=config,
                    
                )
                
                result_json = json.loads(result.model_dump_json(),)
                meta_data = {}
                meta_data["url"] = json.dumps(result_json["url"]).strip('"')
                print(f"url: {meta_data['url']}")

                for key, value in result_json["metadata"].items():
                    meta_data[key] = value
                
                # 指定出力ディレクトリの下にoutputディレクトリを作成してそこにメタデータとマークダウンを保存
                # メタデータとマークダウンは、データベースへのロード処理で一緒に使用する。           
                Path(f"{output_dir}/md").mkdir(parents=True, exist_ok=True)
                # mdディレクトリへメタデータを保存
                with open("{}/md/{}".format(output_dir, url2fname(url) + ".meta"), "w", encoding="utf-8") as file:
                    file.write(json.dumps(meta_data, ensure_ascii=False))

                # mdディレクトリへ加工後のマークダウンを保存
                with open("{}/md/{}".format(output_dir, url2fname(url) + ".md"), "w") as file:
                    file.write(
                        adjust_markdown(result.markdown)
                    )
                # Unspan tables
                unspanner = TableUnspanner(result.html)
                result_list = []
                # Get all tables as markdown
                all_tables = unspanner.get_all_tables()
                for j, table in enumerate(all_tables):
                    markdown = unspanner.to_markdown_compact(table_index=j, header_row=0)
                    result_list.append(f"Table {j+1}:\n{markdown}\n\n\n\n")            

                # mdディレクトリへmarkdownテーブル(colspan/rowspanを展開したもの)を保存
                if len(result_list) > 0:
                    with open("{}/md/{}".format(output_dir, url2fname(url) + "_unspanned_tables.md"), "w", encoding="utf-8") as file:
                        file.write(''.join(result_list))

                # mdディレクトリへrawマークダウンを保存
                if os.getenv("EXCLUDE_RAW_MARKDOWN", "false").lower() == "true":
                    print("Skipping saving raw markdown as per EXCLUDE_RAW_MARKDOWN setting.")
                else:
                    with open("{}/md/{}".format(output_dir, url2fname(url) + "_raw.md"), "w") as file:
                        file.write(result.markdown)

                # 出力ディレクトリ直下へcleaned HTML and JSONを保存
                if os.getenv("EXCLUDE_CLEANED_HTML", "false").lower() == "true":
                    print("Skipping saving cleaned HTML as per EXCLUDE_CLEANED_HTML setting.")
                else:
                    with open("{}/{}".format(output_dir, url2fname(url) + ".html"), "w") as file:
                        # file.write(result.cleaned_html)
                        file.write(result.html)

                if os.getenv("EXCLUDE_JSON", "false").lower() == "true":
                    print("Skipping saving JSON as per EXCLUDE_JSON setting.")
                else:
                    with open("{}/{}".format(output_dir, url2fname(url) + ".json"), "w", encoding="utf-8") as file:
                        file.write(json.dumps(result_json, indent=2, ensure_ascii=False))

                if i > 0 and i % 10 == 0:
                    await asyncio.sleep(0.5)               

                print(f"Processed {i+1}/{len(urls)}: {url}")
            except Exception as e:
                print(f"Error processing {url}: {e}")
                raise e
    
def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert url contents to markdown files"
    )
    parser.add_argument(
        'input_file',
        help='Path to the input url list file'
    )
    parser.add_argument(
        'output_dir',
        # '-o', '--output',
        help='Path to the output directory'
    )
    args = parser.parse_args()

    input_file = args.input_file
    output_dir = args.output_dir

    asyncio.run(crawl(input_file=input_file, output_dir=output_dir))

if __name__ == "__main__":
    main()
    
    

import logging
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Set, Union

import lxml
from bs4 import BeautifulSoup
from docling_core.types.doc import (
    DocItemLabel,
    DoclingDocument,
    DocumentOrigin,
    GroupLabel,
    TableCell,
    TableData,
)
from lxml import etree

from docling.backend.abstract_backend import DeclarativeDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import InputDocument

_log = logging.getLogger(__name__)


class PubMedDocumentBackend(DeclarativeDocumentBackend):
    def __init__(self, in_doc: "InputDocument", path_or_stream: Union[BytesIO, Path]):
        super().__init__(in_doc, path_or_stream)
        self.path_or_stream = path_or_stream

        # Initialize parents for the document hierarchy
        self.parents: dict = {}

    def is_valid(self) -> bool:
        return True

    @classmethod
    def supports_pagination(cls) -> bool:
        return False

    def unload(self):
        if isinstance(self.path_or_stream, BytesIO):
            self.path_or_stream.close()
        self.path_or_stream = None

    @classmethod
    def supported_formats(cls) -> Set[InputFormat]:
        return {InputFormat.PUBMED}

    def convert(self) -> DoclingDocument:
        # Create empty document
        origin = DocumentOrigin(
            filename=self.file.name or "file",
            mimetype="application/xml",
            binary_hash=self.document_hash,
        )
        doc = DoclingDocument(name=self.file.stem or "file", origin=origin)

        _log.debug("Trying to convert XML...")

        # Get parsed XML components
        xml_components: dict = self.parse(str(self.file))

        # Add XML components to the document
        doc = self.populate_document(doc, xml_components)
        return doc

    def parse_title(self, tree: lxml.etree._ElementTree) -> str:
        title: str = " ".join(
            [
                t.replace("\n", "")
                for t in tree.find(".//title-group/article-title").itertext()
            ]
        )
        return title

    def parse_authors(self, tree: lxml.etree._ElementTree) -> List[Dict]:
        # Get mapping between affiliation ids and names
        affiliation_ids = tree.xpath(".//aff[@id]/@id")
        affiliation_names = []
        for affiliation_xml in tree.xpath(".//aff[@id]"):
            affiliation_names.append(
                "".join([t.replace("\n", ":") for t in affiliation_xml.itertext()])
            )
        affiliation_ids_names = {
            id: name for id, name in zip(affiliation_ids, affiliation_names)
        }

        # Get author names and affiliation names
        authors: List[Dict] = []
        for author_xml in tree.xpath(
            './/contrib-group/contrib[@contrib-type="author"]'
        ):
            author = {
                "name": "",
                "affiliation_names": [],
            }

            # Affiliation names
            affiliation_ids = [
                a.attrib["rid"] for a in author_xml.findall('xref[@ref-type="aff"]')
            ]
            for id in affiliation_ids:
                if id in affiliation_ids_names:
                    author["affiliation_names"].append(affiliation_ids_names[id])

            # Name
            author["name"] = (
                author_xml.find("name/surname").text
                + " "
                + author_xml.find("name/given-names").text
            )

            authors.append(author)
        return authors

    def parse_abstract(self, tree: lxml.etree._ElementTree) -> str:
        texts = []
        for abstract_xml in tree.findall(".//abstract"):
            for text in abstract_xml.itertext():
                texts.append(text.replace("\n", "").strip())
        abstract: str = "".join(texts)
        return abstract

    def parse_main_text(self, tree: lxml.etree._ElementTree) -> List[Dict]:
        paragraphs: List[Dict] = []
        for paragraph_xml in tree.xpath("//body//p"):
            # Skip captions
            if "/caption" in paragraph_xml.getroottree().getpath(paragraph_xml):
                continue

            paragraph = {"text": "", "headers": []}

            # Text
            paragraph["text"] = "".join(
                [t.replace("\n", "") for t in paragraph_xml.itertext()]
            )

            # Header
            path = "../title"
            while paragraph_xml.find(path) != None:
                paragraph["headers"].append(
                    "".join(
                        [
                            t.replace("\n", "")
                            for t in paragraph_xml.find(path).itertext()
                        ]
                    )
                )
                path = "../" + path

            paragraphs.append(paragraph)

        return paragraphs

    def parse_tables(self, tree: lxml.etree._ElementTree) -> List[Dict]:
        tables: List[Dict] = []
        for table_xml in tree.xpath(".//body//table-wrap"):
            table = {"label": "", "caption": "", "content": ""}

            # Content
            if table_xml.find("table") != None:
                table_content_xml = table_xml.find("table")
            elif table_xml.find("alternatives/table") != None:
                table_content_xml = table_xml.find("alternatives/table")
            else:
                table_content_xml = None
            if table_content_xml != None:
                table["content"] = etree.tostring(table_content_xml)

            # Caption
            if table_xml.find("caption/p") != None:
                caption_xml = table_xml.find("caption/p")
            elif table_xml.find("caption/title") != None:
                caption_xml = table_xml.find("caption/title")
            else:
                caption_xml = None
            if caption_xml != None:
                table["caption"] = "".join(
                    [t.replace("\n", "") for t in caption_xml.itertext()]
                )

            # Label
            if table_xml.find("label") != None:
                table["label"] = table_xml.find("label").text

            tables.append(table)
        return tables

    def parse_figure_captions(self, tree: lxml.etree._ElementTree) -> List[Dict]:
        figure_captions: List[Dict] = []

        if tree.findall(".//fig") == None:
            return figure_captions

        for figure_xml in tree.findall(".//fig"):
            figure_caption = {
                "caption": "",
                "label": "",
            }

            # Label
            if figure_xml.find("label") != None:
                figure_caption["label"] = "".join(
                    [t.replace("\n", "") for t in figure_xml.find("label").itertext()]
                )

            # Caption
            if figure_xml.find("caption") != None:
                caption = ""
                for caption_xml in figure_xml.find("caption").getchildren():
                    caption += (
                        "".join([t.replace("\n", "") for t in caption_xml.itertext()])
                        + "\n"
                    )
                figure_caption["caption"] = caption

            figure_captions.append(figure_caption)

        return figure_captions

    def parse_references(self, tree: lxml.etree._ElementTree) -> List[Dict]:
        references: List[Dict] = []
        for reference_xml_abs in tree.xpath(".//ref-list/ref[@id]"):
            reference = {
                "author_names": "",
                "title": "",
                "journal": "",
                "year": "",
            }
            reference_xml = None
            for tag in ["mixed-citation", "element-citation", "citation"]:
                reference_xml = reference_xml_abs.find(tag)
                if reference_xml != None:
                    break

            if reference_xml == None:
                continue

            if all(
                not (ref_type in ["citation-type", "publication-type"])
                for ref_type in reference_xml.attrib.keys()
            ):
                continue

            # Author names
            names = []
            if reference_xml.find("name") != None:
                for name_xml in reference_xml.findall("name"):
                    name = [t.text for t in name_xml.getchildren()][::-1]
                    name = " ".join([t for t in name if t != None])
                    names.append(name)
            elif reference_xml.find("person-group") != None:
                for name_xml in reference_xml.find("person-group"):
                    name = " ".join(
                        name_xml.xpath("given-names/text()")
                        + name_xml.xpath("surname/text()")
                    )
                    names.append(name)
            reference["author_names"] = "; ".join(names)

            # Title
            if reference_xml.find("article-title") != None:
                reference["title"] = " ".join(
                    [
                        t.replace("\n", " ")
                        for t in reference_xml.find("article-title").itertext()
                    ]
                )

            # Journal
            if reference_xml.find("source") != None:
                reference["journal"] = reference_xml.find("source").text

            # Year
            if reference_xml.find("year") != None:
                reference["year"] = reference_xml.find("year").text

            if (
                (reference_xml.find("article-title") == None)
                and (reference_xml.find("journal") == None)
                and (reference_xml.find("year") == None)
            ):
                reference["title"] = reference_xml.text

            references.append(reference)
        return references

    def parse(self, filename: str) -> Dict[str, Union[str, List]]:
        """Parsing PubMed document."""
        tree = etree.parse(filename)

        title: str = self.parse_title(tree)
        authors: List[Dict] = self.parse_authors(tree)
        abstract: str = self.parse_abstract(tree)
        paragraphs: List[Dict] = self.parse_main_text(tree)
        tables: List[Dict] = self.parse_tables(tree)
        figure_captions: List[Dict] = self.parse_figure_captions(tree)
        references: List[Dict] = self.parse_references(tree)

        xml_components: Dict[str, Union[str, List]] = {
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "paragraphs": paragraphs,
            "tables": tables,
            "figure_captions": figure_captions,
            "references": references,
        }
        return xml_components

    def populate_document(
        self, doc: DoclingDocument, xml_components: Dict[str, Union[str, List]]
    ) -> DoclingDocument:
        self.add_title(doc, xml_components)
        self.add_authors(doc, xml_components)
        self.add_abstract(doc, xml_components)
        self.add_main_text(doc, xml_components)

        if xml_components["tables"] != []:
            self.add_tables(doc, xml_components)

        if xml_components["figure_captions"] != []:
            self.add_figure_captions(doc, xml_components)

        self.add_references(doc, xml_components)
        return doc

    def add_figure_captions(
        self, doc: DoclingDocument, xml_components: Dict[str, Union[str, List]]
    ) -> None:
        self.parents["Figures"] = doc.add_heading(
            parent=self.parents["Title"], text="Figures"
        )
        for figure_caption_xml_component in xml_components["figure_captions"]:
            figure_caption_text = (
                figure_caption_xml_component["label"]
                + ": "
                + figure_caption_xml_component["caption"]
            )
            fig_caption = doc.add_text(
                label=DocItemLabel.CAPTION, text=figure_caption_text
            )
            doc.add_picture(
                parent=self.parents["Figures"],
                caption=fig_caption,
            )
        return

    def add_title(
        self, doc: DoclingDocument, xml_components: Dict[str, Union[str, List]]
    ) -> None:
        self.parents["Title"] = doc.add_text(
            parent=None,
            text=xml_components["title"],
            label=DocItemLabel.TITLE,
        )
        return

    def add_authors(
        self, doc: DoclingDocument, xml_components: Dict[str, Union[str, List]]
    ) -> None:
        authors_affiliations: list = []
        for author in xml_components["authors"]:
            authors_affiliations.append(author["name"])
            authors_affiliations.append(", ".join(author["affiliation_names"]))
        authors_affiliations_str = "; ".join(authors_affiliations)

        doc.add_text(
            parent=self.parents["Title"],
            text=authors_affiliations_str,
            label=DocItemLabel.PARAGRAPH,
        )
        return

    def add_abstract(
        self, doc: DoclingDocument, xml_components: Dict[str, Union[str, List]]
    ) -> None:
        abstract_text: str = xml_components["abstract"].replace("\n", " ").strip()
        if abstract_text.strip():
            self.parents["Abstract"] = doc.add_heading(
                parent=self.parents["Title"], text="Abstract"
            )
            doc.add_text(
                parent=self.parents["Abstract"],
                text=abstract_text,
                label=DocItemLabel.TEXT,
            )
        return

    def add_main_text(
        self, doc: DoclingDocument, xml_components: Dict[str, Union[str, List]]
    ) -> None:
        added_headers: list = []
        for paragraph in xml_components["paragraphs"]:
            if paragraph["headers"] == []:
                continue

            # Add header
            for i, header in enumerate(reversed(paragraph["headers"])):
                if header in added_headers:
                    continue
                added_headers.append(header)

                if ((i - 1) >= 0) and list(reversed(paragraph["headers"]))[
                    i - 1
                ] in self.parents:
                    parent = self.parents[list(reversed(paragraph["headers"]))[i - 1]]
                else:
                    parent = self.parents["Title"]

                self.parents[header] = doc.add_heading(parent=parent, text=header)

            # Add paragraph text
            if paragraph["headers"][0] in self.parents:
                parent = self.parents[paragraph["headers"][0]]
            else:
                parent = self.parents["Title"]

            doc.add_text(parent=parent, label=DocItemLabel.TEXT, text=paragraph["text"])
        return

    def add_references(
        self, doc: DoclingDocument, xml_components: Dict[str, Union[str, List]]
    ) -> None:
        self.parents["References"] = doc.add_heading(
            parent=self.parents["Title"], text="References"
        )
        current_list = doc.add_group(
            parent=self.parents["References"], label=GroupLabel.LIST, name="list"
        )
        for reference in xml_components["references"]:
            reference_text: str = ""
            if reference["author_names"] != "":
                reference_text += reference["author_names"] + ". "

            if reference["title"] != "":
                reference_text += reference["title"]
                if reference["title"][-1] != ".":
                    reference_text += "."
                reference_text += " "

            if reference["journal"] != "":
                reference_text += reference["journal"]

            if reference["year"] != "":
                reference_text += " (" + reference["year"] + ")"

            if reference_text == "":
                _log.debug(f"Skipping reference for: {str(self.file)}")
                continue

            doc.add_list_item(
                text=reference_text, enumerated=False, parent=current_list
            )
        return

    def add_tables(
        self, doc: DoclingDocument, xml_components: Dict[str, Union[str, List]]
    ) -> None:
        self.parents["Tables"] = doc.add_heading(
            parent=self.parents["Title"], text="Tables"
        )
        for table_xml_component in xml_components["tables"]:
            try:
                self.add_table(doc, table_xml_component)
            except Exception as e:
                _log.debug(f"Skipping unsupported table for: {str(self.file)}")
                pass
        return

    def add_table(
        self, doc: DoclingDocument, table_xml_component: Dict[str, Union[str, List]]
    ) -> None:
        table_xml = table_xml_component["content"].decode("utf-8")
        soup = BeautifulSoup(table_xml, "html.parser")
        table_tag = soup.find("table")

        nested_tables = table_tag.find("table")
        if nested_tables != None:
            _log.debug(f"Skipping nested table for: {str(self.file)}")
            return

        # Count the number of rows (number of <tr> elements)
        num_rows = len(table_tag.find_all("tr"))

        # Find the number of columns (taking into account colspan)
        num_cols = 0
        for row in table_tag.find_all("tr"):
            col_count = 0
            for cell in row.find_all(["td", "th"]):
                colspan = int(cell.get("colspan", 1))
                col_count += colspan
            num_cols = max(num_cols, col_count)

        grid = [[None for _ in range(num_cols)] for _ in range(num_rows)]

        data = TableData(num_rows=num_rows, num_cols=num_cols, table_cells=[])

        # Iterate over the rows in the table
        for row_idx, row in enumerate(table_tag.find_all("tr")):
            # For each row, find all the column cells (both <td> and <th>)
            cells = row.find_all(["td", "th"])

            # Check if each cell in the row is a header -> means it is a column header
            col_header = True
            for j, html_cell in enumerate(cells):
                if html_cell.name == "td":
                    col_header = False

            # Extract and print the text content of each cell
            col_idx = 0
            for _, html_cell in enumerate(cells):
                text = html_cell.text

                col_span = int(html_cell.get("colspan", 1))
                row_span = int(html_cell.get("rowspan", 1))

                while grid[row_idx][col_idx] != None:
                    col_idx += 1
                for r in range(row_span):
                    for c in range(col_span):
                        grid[row_idx + r][col_idx + c] = text

                cell = TableCell(
                    text=text,
                    row_span=row_span,
                    col_span=col_span,
                    start_row_offset_idx=row_idx,
                    end_row_offset_idx=row_idx + row_span,
                    start_col_offset_idx=col_idx,
                    end_col_offset_idx=col_idx + col_span,
                    col_header=col_header,
                    row_header=((not col_header) and html_cell.name == "th"),
                )
                data.table_cells.append(cell)

        table_caption = doc.add_text(
            label=DocItemLabel.CAPTION,
            text=table_xml_component["label"] + ": " + table_xml_component["caption"],
        )
        doc.add_table(data=data, parent=self.parents["Tables"], caption=table_caption)
        return

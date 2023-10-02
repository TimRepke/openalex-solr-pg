import csv
import gzip
import time
import logging
from pathlib import Path
from typing import TextIO

from msgspec.json import Decoder, Encoder
from msgspec import DecodeError

from processors.postgres import structs
from processors.postgres.deletion import generate_deletions
from shared.config import settings
from shared.util import strip_id
from shared.cyth.invert_index import invert


def fieldnames(writer: csv.DictWriter):
    return ','.join(writer.fieldnames)


def prepare_list(lst: list[str] | None, strip: bool = False) -> str | None:
    def clean(string):
        return string.replace(',', '').replace('"', '').replace("'", '')

    if lst is not None and len(lst) > 0:
        prepped = ([strip_id(clean(li))
                    for li in lst
                    if li is not None and len(li) > 0] if strip else
                   [clean(li)
                    for li in lst
                    if li is not None and len(li) > 0])
        if len(prepped) > 0:
            return '{' + ','.join(prepped) + '}'

    return None


def get_writer(buffer: TextIO, fields: list[str]) -> csv.DictWriter:
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    return writer


def flatten_authors_partition(partition: Path | str,
                              out_sql_cpy: Path | str,
                              out_sql_del: Path | str,
                              out_authors: Path | str,
                              preserve_ram: bool):
    logging.info(f'Flattening partition file {partition}')
    partition: Path = Path(partition)
    out_sql_cpy: Path = Path(out_sql_cpy)
    out_sql_del: Path = Path(out_sql_del)
    out_authors: Path = Path(out_authors)
    startTime = time.time()

    with (gzip.open(out_authors, 'wt', encoding='utf-8') as f_authors,
          open(out_sql_del, 'w') as f_sql_del,
          open(out_sql_cpy, 'w') as f_sql_cpy,
          gzip.open(partition, 'rb') as f_in):
        writer_authors = get_writer(f_authors, ['id',
                                                'cited_by_count', 'works_count', 'h_index', 'i10_index',
                                                'display_name', 'display_name_alternatives',
                                                'id_mag', 'id_orcid', 'id_scopus', 'id_twitter', 'id_wikipedia',
                                                'created_date', 'updated_date'])

        decoder = Decoder(structs.Author)

        n_authors = 0
        author_ids = []

        if preserve_ram:
            lines = f_in
        else:
            lines = f_in.readlines()

        for line in lines:
            n_authors += 1
            author = decoder.decode(line)
            aid = author.id[21:]
            author_ids.append(aid)

            writer_authors.writerow({
                'id': aid,
                'cited_by_count': author.cited_by_count,
                'works_count': author.works_count,
                'h_index': author.summary_stats.h_index,
                'i10_index': author.summary_stats.i10_index,
                'display_name': author.display_name,
                'display_name_alternatives': prepare_list(author.display_name_alternatives),
                'last_known_institution': (strip_id(author.last_known_institution.id)
                                           if author.last_known_institution is not None else None),
                'id_mag': author.ids.mag,
                'id_orcid': author.ids.orcid,
                'id_scopus': author.ids.scopus,
                'id_twitter': author.ids.twitter,
                'id_wikipedia': author.ids.wikipedia,
                'created_date': author.created_date,
                'updated_date': author.updated_date
            })

        for del_row in generate_deletions(ids=author_ids, object_type='author', batch_size=1000):
            f_sql_del.write(del_row + '\n')

        f_sql_cpy.write(f"COPY {settings.pg_schema}.authors ({fieldnames(writer_authors)}) "
                        f"FROM PROGRAM 'gunzip -c {out_authors.absolute()}' csv header;\n\n")

    executionTime = (time.time() - startTime)
    mins = int(executionTime / 60)
    secs = executionTime - (mins * 60)
    logging.info(f'Flattened {n_authors:,} authors in '
                 f'{mins}:{secs:.2f} from {partition}')


def flatten_institutions_partition(partition: Path | str,
                                   out_sql_cpy: Path | str,
                                   out_sql_del: Path | str,
                                   out_institutions: Path | str,
                                   out_m2m_association: Path | str,
                                   out_m2m_concepts: Path | str,
                                   preserve_ram: bool):
    logging.info(f'Flattening partition file {partition}')
    partition: Path = Path(partition)
    out_sql_cpy: Path = Path(out_sql_cpy)
    out_sql_del: Path = Path(out_sql_del)
    out_institutions: Path = Path(out_institutions)
    out_m2m_association: Path = Path(out_m2m_association)
    out_m2m_concepts: Path = Path(out_m2m_concepts)
    startTime = time.time()

    with (gzip.open(out_institutions, 'wt', encoding='utf-8') as f_inst,
          gzip.open(out_m2m_association, 'wt', encoding='utf-8') as f_m2m_ass,
          gzip.open(out_m2m_concepts, 'wt', encoding='utf-8') as f_m2m_con,
          open(out_sql_del, 'w') as f_sql_del,
          open(out_sql_cpy, 'w') as f_sql_cpy,
          gzip.open(partition, 'rb') as f_in):
        writer_inst = get_writer(f_inst, ['id', 'type', 'homepage_url',
                                          'cited_by_count', 'works_count', 'h_index', 'i10_index',
                                          'display_name', 'display_name_alternatives', 'display_name_acronyms',
                                          'id_ror', 'id_mag', 'id_wikipedia', 'id_wikidata', 'id_grid',
                                          'city', 'geonames_city_id', 'region', 'country', 'country_code',
                                          'latitude', 'longitude',
                                          'created_date', 'updated_date'])
        writer_m2m_ass = get_writer(f_m2m_ass, ['institution_a_id', 'institution_b_id', 'relationship'])
        writer_m2m_con = get_writer(f_m2m_con, ['institution_id', 'concept_id', 'score'])

        decoder = Decoder(structs.Institution)

        n_institutions = 0
        institution_ids = []

        if preserve_ram:
            lines = f_in
        else:
            lines = f_in.readlines()

        for line in lines:
            n_institutions += 1
            institution: structs.Institution = decoder.decode(line)
            iid = institution.id[21:]
            institution_ids.append(iid)

            writer_inst.writerow({
                'id': iid,
                'type': institution.type,
                'homepage_url': institution.homepage_url,
                'cited_by_count': institution.cited_by_count,
                'works_count': institution.works_count,
                'h_index': institution.summary_stats.h_index,
                'i10_index': institution.summary_stats.i10_index,
                'display_name': institution.display_name,
                'display_name_alternatives': prepare_list(institution.display_name_alternatives),
                'display_name_acronyms': prepare_list(institution.display_name_acronyms),
                'id_ror': institution.ror,
                'id_mag': institution.ids.mag,
                'id_wikipedia': institution.ids.wikipedia,
                'id_wikidata': institution.ids.wikidata,
                'id_grid': institution.ids.grid,
                'city': institution.geo.city,
                'geonames_city_id': institution.geo.geonames_city_id,
                'region': institution.geo.region,
                'country': institution.geo.country,
                'country_code': institution.geo.country_code,
                'latitude': institution.geo.latitude,
                'longitude': institution.geo.longitude,
                'created_date': institution.created_date,
                'updated_date': institution.updated_date
            })

            for ass in institution.associated_institutions:
                writer_m2m_ass.writerow({
                    'institution_a_id': iid,
                    'institution_b_id': ass.id[21:],
                    'relationship': ass.relationship
                })

            for con in institution.x_concepts:
                writer_m2m_con.writerow({
                    'institution_id': iid,
                    'concept_id': con.id[21:],
                    'score': con.score
                })

        for del_row in generate_deletions(ids=institution_ids, object_type='institution', batch_size=1000):
            f_sql_del.write(del_row + '\n')

        f_sql_cpy.write(f"COPY {settings.pg_schema}.institutions ({fieldnames(writer_inst)}) "
                        f"FROM PROGRAM 'gunzip -c {out_institutions.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.institutions_associations ({fieldnames(writer_m2m_ass)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_association.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.institutions_concepts ({fieldnames(writer_m2m_con)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_concepts.absolute()}' csv header;\n\n")

    executionTime = (time.time() - startTime)
    mins = int(executionTime / 60)
    secs = executionTime - (mins * 60)
    logging.info(f'Flattened {n_institutions:,} institutions in '
                 f'{mins}:{secs:.2f} from {partition}')


def flatten_publisher_partition(partition: Path | str,
                                out_sql_cpy: Path | str,
                                out_sql_del: Path | str,
                                out_publishers: Path | str,
                                preserve_ram: bool):
    logging.info(f'Flattening partition file {partition}')
    partition: Path = Path(partition)
    out_sql_cpy: Path = Path(out_sql_cpy)
    out_sql_del: Path = Path(out_sql_del)
    out_publishers: Path = Path(out_publishers)
    startTime = time.time()

    with (gzip.open(out_publishers, 'wt', encoding='utf-8') as f_pub,
          open(out_sql_del, 'w') as f_sql_del,
          open(out_sql_cpy, 'w') as f_sql_cpy,
          gzip.open(partition, 'rb') as f_in):
        writer_pub = csv.DictWriter(f_pub,
                                    fieldnames=['id',
                                                'cited_by_count', 'works_count', 'h_index', 'i10_index',
                                                'display_name', 'alternate_titles', 'country_codes',
                                                'id_ror', 'id_wikidata', 'hierarchy_level', 'lineage', 'parent',
                                                'created_date', 'updated_date'],
                                    extrasaction='ignore')
        writer_pub.writeheader()

        decoder = Decoder(structs.Publisher)

        n_pubs = 0
        publisher_ids = []

        if preserve_ram:
            lines = f_in
        else:
            lines = f_in.readlines()

        for line in lines:
            publisher = decoder.decode(line)
            pid = strip_id(publisher.id)

            # There are very few publishers with no title, drop them!
            if pid is None:
                continue

            n_pubs += 1
            publisher_ids.append(pid)

            writer_pub.writerow({
                'id': pid,
                'cited_by_count': publisher.cited_by_count,
                'works_count': publisher.works_count,
                'h_index': publisher.summary_stats.h_index,
                'i10_index': publisher.summary_stats.i10_index,
                'display_name': publisher.display_name,
                'alternate_titles': prepare_list(publisher.alternate_titles),
                'country_codes': prepare_list(publisher.country_codes),
                'id_ror': publisher.ids.ror,
                'id_wikidata': publisher.ids.wikidata,
                'hierarchy_level': publisher.hierarchy_level,
                'parent': strip_id(publisher.parent_publisher.id) if publisher.parent_publisher is not None else None,
                'lineage': prepare_list(publisher.lineage, strip=True),
                'created_date': publisher.created_date,
                'updated_date': publisher.updated_date
            })

        for del_row in generate_deletions(ids=publisher_ids, object_type='publisher', batch_size=1000):
            f_sql_del.write(del_row + '\n')

        f_sql_cpy.write(f"COPY {settings.pg_schema}.publishers ({fieldnames(writer_pub)}) "
                        f"FROM PROGRAM 'gunzip -c {out_publishers.absolute()}' csv header;\n\n")

    executionTime = (time.time() - startTime)
    mins = int(executionTime / 60)
    secs = executionTime - (mins * 60)
    logging.info(f'Flattened {n_pubs:,} publishers in '
                 f'{mins}:{secs:.2f} from {partition}')


def flatten_funder_partition(partition: Path | str,
                             out_sql_cpy: Path | str,
                             out_sql_del: Path | str,
                             out_funders: Path | str,
                             preserve_ram: bool):
    logging.info(f'Flattening partition file {partition}')
    partition: Path = Path(partition)
    out_sql_cpy: Path = Path(out_sql_cpy)
    out_sql_del: Path = Path(out_sql_del)
    out_funders: Path = Path(out_funders)
    startTime = time.time()

    with (gzip.open(out_funders, 'wt', encoding='utf-8') as f_funders,
          open(out_sql_del, 'w') as f_sql_del,
          open(out_sql_cpy, 'w') as f_sql_cpy,
          gzip.open(partition, 'rb') as f_in):
        writer_funders = csv.DictWriter(f_funders,
                                        fieldnames=['id',
                                                    'cited_by_count', 'works_count', 'h_index', 'i10_index',
                                                    'display_name', 'alternate_titles', 'description', 'homepage_url',
                                                    'id_ror', 'id_wikidata', 'id_crossref', 'id_doi',
                                                    'created_date', 'updated_date'],
                                        extrasaction='ignore')
        writer_funders.writeheader()

        decoder = Decoder(structs.Funder)

        n_funders = 0
        funder_ids = []

        if preserve_ram:
            lines = f_in
        else:
            lines = f_in.readlines()

        for line in lines:
            n_funders += 1
            funder = decoder.decode(line)
            fid = strip_id(funder.id)
            funder_ids.append(fid)

            writer_funders.writerow({
                'id': fid,
                'cited_by_count': funder.cited_by_count,
                'works_count': funder.works_count,
                'h_index': funder.summary_stats.h_index,
                'i10_index': funder.summary_stats.i10_index,
                'display_name': funder.display_name,
                'alternate_titles': prepare_list(funder.alternate_titles),
                'description': funder.description,
                'homepage_url': funder.homepage_url,
                'id_ror': funder.ids.ror,
                'id_wikidata': funder.ids.wikidata,
                'id_crossref': funder.ids.crossref,
                'id_doi': funder.ids.doi,
                'created_date': funder.created_date,
                'updated_date': funder.updated_date
            })

        for del_row in generate_deletions(ids=funder_ids, object_type='funder', batch_size=1000):
            f_sql_del.write(del_row + '\n')

        f_sql_cpy.write(f"COPY {settings.pg_schema}.funders ({fieldnames(writer_funders)}) "
                        f"FROM PROGRAM 'gunzip -c {out_funders.absolute()}' csv header;\n\n")

    executionTime = (time.time() - startTime)
    mins = int(executionTime / 60)
    secs = executionTime - (mins * 60)
    logging.info(f'Flattened {n_funders:,} funders in '
                 f'{mins}:{secs:.2f} from {partition}')


def flatten_concept_partition(partition: Path | str,
                              out_sql_cpy: Path | str,
                              out_sql_del: Path | str,
                              out_concepts: Path | str,
                              out_m2m_ancestor: Path | str,
                              out_m2m_related: Path | str,
                              preserve_ram: bool):
    logging.info(f'Flattening partition file {partition}')
    partition: Path = Path(partition)
    out_sql_cpy: Path = Path(out_sql_cpy)
    out_sql_del: Path = Path(out_sql_del)
    out_concepts: Path = Path(out_concepts)
    out_m2m_ancestor: Path = Path(out_m2m_ancestor)
    out_m2m_related: Path = Path(out_m2m_related)
    startTime = time.time()

    with (gzip.open(out_concepts, 'wt', encoding='utf-8') as f_concepts,
          gzip.open(out_m2m_related, 'wt', encoding='utf-8') as f_m2m_related,
          gzip.open(out_m2m_ancestor, 'wt', encoding='utf-8') as f_m2m_ancestor,
          open(out_sql_del, 'w') as f_sql_del,
          open(out_sql_cpy, 'w') as f_sql_cpy,
          gzip.open(partition, 'rb') as f_in):
        writer_concepts = get_writer(f_concepts, ['id',
                                                  'cited_by_count', 'works_count', 'h_index', 'i10_index',
                                                  'display_name', 'description', 'level',
                                                  'id_mag', 'id_umls_cui', 'id_umls_aui',
                                                  'id_wikidata', 'id_wikipedia',
                                                  'created_date', 'updated_date'])
        writer_m2m_related = get_writer(f_m2m_related, ['concept_a_id', 'concept_b_id', 'score'])
        writer_m2m_ancestor = get_writer(f_m2m_ancestor, ['concept_a_id', 'concept_b_id'])

        decoder = Decoder(structs.Concept)

        n_concepts = 0
        concept_ids = []

        if preserve_ram:
            lines = f_in
        else:
            lines = f_in.readlines()

        for line in lines:
            n_concepts += 1
            concept = decoder.decode(line)
            cid = strip_id(concept.id)
            concept_ids.append(cid)

            writer_concepts.writerow({
                'id': concept.id,
                'cited_by_count': concept.cited_by_count,
                'works_count': concept.works_count,
                'h_index': concept.summary_stats.h_index,
                'i10_index': concept.summary_stats.i10_index,
                'display_name': concept.display_name,
                'description': concept.description,
                'level': concept.level,
                'id_mag': concept.ids.mag,
                'id_umls_cui': prepare_list(concept.ids.umls_cui),
                'id_umls_aui': prepare_list(concept.ids.umls_aui),
                'id_wikidata': concept.ids.wikidata,
                'id_wikipedia': concept.ids.wikipedia,
                'created_date': concept.created_date,
                'updated_date': concept.updated_date
            })

            for rel in concept.related_concepts:
                writer_m2m_related.writerow({
                    'concept_a_id': cid,
                    'concept_b_id': strip_id(rel.id),
                    'score': rel.score
                })
            for anc in concept.related_concepts:
                writer_m2m_ancestor.writerow({
                    'concept_a_id': cid,
                    'concept_b_id': strip_id(anc.id)
                })

        for del_row in generate_deletions(ids=concept_ids, object_type='concept', batch_size=1000):
            f_sql_del.write(del_row + '\n')

        f_sql_cpy.write(f"COPY {settings.pg_schema}.concepts ({fieldnames(writer_concepts)}) "
                        f"FROM PROGRAM 'gunzip -c {out_concepts.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.concepts_ancestors ({fieldnames(writer_m2m_ancestor)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_ancestor.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.concepts_related ({fieldnames(writer_m2m_related)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_related.absolute()}' csv header;\n\n")

    executionTime = (time.time() - startTime)
    mins = int(executionTime / 60)
    secs = executionTime - (mins * 60)
    logging.info(f'Flattened {n_concepts:,} concepts in '
                 f'{mins}:{secs:.2f} from {partition}')


def flatten_sources_partition(partition: Path | str,
                              out_sql_cpy: Path | str,
                              out_sql_del: Path | str,
                              out_sources: Path | str,
                              preserve_ram: bool):
    logging.info(f'Flattening partition file {partition}')
    partition: Path = Path(partition)
    out_sql_cpy: Path = Path(out_sql_cpy)
    out_sql_del: Path = Path(out_sql_del)
    out_sources: Path = Path(out_sources)
    startTime = time.time()

    with (gzip.open(out_sources, 'wt', encoding='utf-8') as f_sources,
          open(out_sql_del, 'w') as f_sql_del,
          open(out_sql_cpy, 'w') as f_sql_cpy,
          gzip.open(partition, 'rb') as f_in):
        writer_sources = csv.DictWriter(f_sources,
                                        fieldnames=['id',
                                                    'cited_by_count', 'works_count', 'h_index', 'i10_index',
                                                    'display_name', 'abbreviated_title', 'alternate_titles',
                                                    'country_code', 'homepage_url', 'type', 'apc_usd',
                                                    'host_organization', 'host_organization_name',
                                                    'host_organization_lineage', 'societies',
                                                    'is_in_doaj', 'is_oa',
                                                    'id_mag', 'id_fatcat', 'id_issn', 'id_issn_l', 'id_wikidata',
                                                    'created_date', 'updated_date'],
                                        extrasaction='ignore')
        writer_sources.writeheader()

        decoder = Decoder(structs.Source)

        n_sources = 0
        source_ids = []

        if preserve_ram:
            lines = f_in
        else:
            lines = f_in.readlines()

        for line in lines:
            n_sources += 1
            source = decoder.decode(line)
            sid = strip_id(source.id)
            source_ids.append(sid)

            societies = None
            if source.societies is not None and len(source.societies) > 0:
                societies = [s.organization for s in source.societies]

            writer_sources.writerow({
                'id': sid,
                'cited_by_count': source.cited_by_count,
                'works_count': source.works_count,
                'h_index': source.summary_stats.h_index,
                'i10_index': source.summary_stats.i10_index,
                'display_name': source.display_name,
                'abbreviated_title': source.abbreviated_title,
                'alternate_titles': prepare_list(source.alternate_titles),
                'country_code': source.country_code,
                'homepage_url': source.homepage_url,
                'type': source.type,
                'apc_usd': source.apc_usd,
                'host_organization': source.host_organization,
                'host_organization_name': source.host_organization_name,
                'host_organization_lineage': prepare_list(source.host_organization_lineage, strip=True),
                'societies': prepare_list(societies),
                'is_in_doaj': source.is_in_doaj,
                'is_oa': source.is_oa,
                'id_mag': source.ids.mag,
                'id_fatcat': source.ids.fatcat,
                'id_issn': prepare_list(source.ids.issn),
                'id_issn_l': source.ids.issn_l,
                'id_wikidata': source.ids.wikidata,
                'created_date': source.created_date,
                'updated_date': source.updated_date
            })

        for del_row in generate_deletions(ids=source_ids, object_type='source', batch_size=1000):
            f_sql_del.write(del_row + '\n')

        f_sql_cpy.write(f"COPY {settings.pg_schema}.sources ({fieldnames(writer_sources)}) "
                        f"FROM PROGRAM 'gunzip -c {out_sources.absolute()}' csv header;\n\n")

    executionTime = (time.time() - startTime)
    mins = int(executionTime / 60)
    secs = executionTime - (mins * 60)
    logging.info(f'Flattened {n_sources:,} sources in'
                 f' {mins}:{secs:.2f} from {partition}')


def flatten_works_partition(partition: Path | str,
                            out_sql_cpy: Path | str,
                            out_sql_del: Path | str,
                            out_works: Path | str,
                            out_m2m_locations: Path | str,
                            out_m2m_concepts: Path | str,
                            out_m2m_authorships: Path | str,
                            out_m2m_authorship_institutions: Path | str,
                            out_m2m_references: Path | str,
                            out_m2m_related: Path | str,
                            out_m2m_sdgs: Path | str,
                            preserve_ram: bool):
    logging.info(f'Flattening partition file {partition}')
    partition: Path = Path(partition)
    out_sql_cpy: Path = Path(out_sql_cpy)
    out_sql_del: Path = Path(out_sql_del)
    out_works: Path = Path(out_works)
    out_m2m_locations: Path = Path(out_m2m_locations)
    out_m2m_concepts: Path = Path(out_m2m_concepts)
    out_m2m_authorships: Path = Path(out_m2m_authorships)
    out_m2m_authorship_institutions: Path = Path(out_m2m_authorship_institutions)
    out_m2m_references: Path = Path(out_m2m_references)
    out_m2m_related: Path = Path(out_m2m_related)
    out_m2m_sdgs: Path = Path(out_m2m_sdgs)
    startTime = time.time()

    with (gzip.open(out_works, 'wt', encoding='utf-8') as f_works,
          gzip.open(out_m2m_locations, 'wt', encoding='utf-8') as f_locations,
          gzip.open(out_m2m_concepts, 'wt', encoding='utf-8') as f_concepts,
          gzip.open(out_m2m_authorships, 'wt', encoding='utf-8') as f_authorships,
          gzip.open(out_m2m_authorship_institutions, 'wt', encoding='utf-8') as f_authorship_institutions,
          gzip.open(out_m2m_references, 'wt', encoding='utf-8') as f_references,
          gzip.open(out_m2m_related, 'wt', encoding='utf-8') as f_related,
          gzip.open(out_m2m_sdgs, 'wt', encoding='utf-8') as f_sdgs,
          open(out_sql_del, 'w') as f_sql_del,
          open(out_sql_cpy, 'w') as f_sql_cpy,
          gzip.open(partition, 'rb') as f_in):
        writer_works = get_writer(f_works, [
            'id',
            'title', 'abstract', 'display_name', 'language', 'publication_date', 'publication_year',
            'volume', 'issue', 'first_page', 'last_page', 'primary_location', 'type', 'type_crossref',
            'id_doi', 'id_mag', 'id_pmid', 'id_pmcid',
            'is_oa', 'oa_status', 'oa_url', 'oa_any_repository_has_fulltext',
            'apc_paid', 'apc_list', 'license', 'cited_by_count',
            'is_paratext', 'is_retracted', 'mesh', 'grants',
            'created_date', 'updated_date'])
        writer_authorships = get_writer(f_authorships, ['work_id', 'author_id', 'position', 'exact_position',
                                                        'raw_author_name', 'raw_affiliation', 'is_corresponding'])
        writer_authorship_institutions = get_writer(f_authorship_institutions, ['work_id', 'author_id',
                                                                                'institution_id'])
        writer_locations = get_writer(f_locations, ['work_id', 'source_id', 'is_oa', 'landing_page_url',
                                                    'license', 'pdf_url', 'version'])
        writer_concepts = get_writer(f_concepts, ['work_id', 'concept_id', 'score'])
        writer_references = get_writer(f_references, ['work_a_id', 'work_b_id'])
        writer_related = get_writer(f_related, ['work_a_id', 'work_b_id'])
        writer_sdgs = get_writer(f_sdgs, ['work_id', 'sdg_id', 'display_name', 'score'])

        decoder = Decoder(structs.Work)
        encoder = Encoder()
        n_works = 0
        n_abstracts = 0
        work_ids = []

        if preserve_ram:
            lines = f_in
        else:
            lines = f_in.readlines()

        for line in lines:
            n_works += 1
            work = decoder.decode(line)
            wid = strip_id(work.id)
            work_ids.append(wid)

            abstract = None
            if work.abstract_inverted_index is not None:
                try:
                    abstract = invert(work.abstract_inverted_index)
                    if len(abstract.strip()) > 0:
                        n_abstracts += 1
                    else:
                        abstract = None
                except DecodeError:
                    logging.warning(f'Failed to read abstract for {wid} in {partition}')
                    abstract = None

            grants = None
            if work.grants is not None and len(work.grants) > 0:
                grants = encoder.encode([{
                    'funder': strip_id(g.funder),
                    'funder_display_name': g.funder_display_name,
                    'award_id': g.award_id
                } for g in work.grants]).decode()

            writer_works.writerow({
                'id': wid,
                'title': work.title,
                'abstract': abstract,
                'display_name': work.display_name,
                'language': work.language,
                'publication_date': work.publication_date,
                'publication_year': work.publication_year,
                'volume': work.biblio.volume,
                'issue': work.biblio.issue,
                'first_page': work.biblio.first_page,
                'last_page': work.biblio.last_page,
                'primary_location': (strip_id(work.primary_location.source.id)
                                     if work.primary_location is not None
                                        and work.primary_location.source is not None else None),
                'type': work.type,
                'type_crossref': work.type_crossref,
                'cited_by_count': work.cited_by_count,
                'id_doi': work.ids.doi,
                'id_mag': work.ids.mag,
                'id_pmid': work.ids.pmid,
                'id_pmcid': work.ids.pmcid,
                'is_oa': work.is_oa,
                'oa_status': work.open_access.oa_status,
                'oa_url': work.open_access.oa_url,
                'oa_any_repository_has_fulltext': work.open_access.any_repository_has_fulltext,
                'apc_paid': (work.apc_paid.value_usd
                             if work.apc_paid is not None and not isinstance(work.apc_paid, list) else None),
                'apc_list': (work.apc_list.value_usd
                             if work.apc_list is not None else None),
                'license': work.license,
                'is_paratext': work.is_paratext,
                'is_retracted': work.is_retracted,
                'mesh': (encoder.encode(work.mesh).decode()
                         if work.mesh is not None and len(work.mesh) > 0 else None),
                'grants': grants,
                'created_date': work.created_date,
                'updated_date': work.updated_date
            })

            for ai, author in enumerate(work.authorships):
                aid = strip_id(author.author.id) if author.author is not None else None
                writer_authorships.writerow({
                    'work_id': wid,
                    'author_id': aid,
                    'position': author.author_position,
                    'exact_position': ai,
                    'raw_affiliation': author.raw_affiliation_string,
                    'raw_author_name': author.raw_author_name,
                    'is_corresponding': author.is_corresponding
                })
                if author.institutions is not None and len(author.institutions) > 0:
                    for institution in author.institutions:
                        writer_authorship_institutions.writerow({
                            'work_id': wid,
                            'author_id': aid,
                            'institution_id': strip_id(institution.id)
                        })

            if work.locations is not None:
                for location in work.locations:
                    writer_locations.writerow({
                        'work_id': wid,
                        'source_id': strip_id(location.source.id) if location.source is not None else None,
                        'is_oa': location.is_oa,
                        'landing_page_url': location.landing_page_url,
                        'is_primary': (work.primary_location is not None
                                       and work.primary_location.source is not None
                                       and location.source is not None
                                       and work.primary_location.source.id == location.source.id
                                       and work.primary_location.source.display_name == location.source.display_name
                                       and work.primary_location.pdf_url == location.pdf_url
                                       and work.primary_location.version == location.version),
                        'license': location.license,
                        'pdf_url': location.pdf_url,
                        'version': location.version
                    })

            for concept in work.concepts:
                writer_concepts.writerow({
                    'work_id': wid,
                    'concept_id': strip_id(concept.id),
                    'score': concept.score
                })
            if work.sustainable_development_goals is not None and len(work.sustainable_development_goals) > 0:
                for sdg in work.sustainable_development_goals:
                    writer_sdgs.writerow({
                        'work_id': wid,
                        'sdg_id': sdg.id,
                        'display_name': sdg.display_name,
                        'score': sdg.score
                    })
            for ref in work.referenced_works:
                writer_references.writerow({
                    'work_a_id': wid,
                    'work_b_id': strip_id(ref)
                })
            for rel in work.related_works:
                writer_related.writerow({
                    'work_a_id': wid,
                    'work_b_id': strip_id(rel)
                })

        for del_row in generate_deletions(ids=work_ids, object_type='work', batch_size=1000):
            f_sql_del.write(del_row + '\n')

        f_sql_cpy.write(f"COPY {settings.pg_schema}.works ({fieldnames(writer_works)}) "
                        f"FROM PROGRAM 'gunzip -c {out_works.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.works_locations ({fieldnames(writer_locations)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_locations.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.works_concepts "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_concepts.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.works_authorships ({fieldnames(writer_authorships)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_authorships.absolute()}' csv header;\n\n")
        f_sql_cpy.write(
            f"COPY {settings.pg_schema}.works_authorship_institutions ({fieldnames(writer_authorship_institutions)}) "
            f"FROM PROGRAM 'gunzip -c {out_m2m_authorship_institutions.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.works_references ({fieldnames(writer_references)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_references.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.works_related ({fieldnames(writer_related)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_related.absolute()}' csv header;\n\n")
        f_sql_cpy.write(f"COPY {settings.pg_schema}.works_sdgs ({fieldnames(writer_sdgs)}) "
                        f"FROM PROGRAM 'gunzip -c {out_m2m_sdgs.absolute()}' csv header;\n\n")

    executionTime = (time.time() - startTime)
    mins = int(executionTime / 60)
    secs = executionTime - (mins * 60)
    logging.info(f'Flattened {n_works:,} works with {n_abstracts:,} abstracts in'
                 f' {mins}:{secs:.2f} from {partition}')


def flatten_authors_partition_kw(kwargs):
    return flatten_authors_partition(**kwargs)


def flatten_institutions_partition_kw(kwargs):
    return flatten_institutions_partition(**kwargs)


def flatten_publisher_partition_kw(kwargs):
    return flatten_publisher_partition(**kwargs)


def flatten_funder_partition_kw(kwargs):
    return flatten_funder_partition(**kwargs)


def flatten_concept_partition_kw(kwargs):
    return flatten_concept_partition(**kwargs)


def flatten_sources_partition_kw(kwargs):
    return flatten_sources_partition(**kwargs)


def flatten_works_partition_kw(kwargs):
    return flatten_works_partition(**kwargs)


if __name__ == '__main__':
    # flatten_works_partition(partition='../data/work/part_001.gz',
    #                         out_works='../data/work/out/wrks.csv.gz',
    #                         out_sql_cpy='../data/work/out/wrks-cpy.sql',
    #                         out_sql_del='../data/work/out/wrks-del.sql',
    #                         out_m2m_concepts='../data/work/out/con.csv.gz',
    #                         out_m2m_related='../data/work/out/rel.csv.gz',
    #                         out_m2m_authorships='../data/work/out/aut.csv.gz',
    #                         out_m2m_locations='../data/work/out/loc.csv.gz',
    #                         out_m2m_references='../data/work/out/ref.csv.gz',
    #                         preserve_ram=True)
    flatten_sources_partition(partition='../data/source/part_000.gz',
                              out_sources='../data/source/out/src.csv.gz',
                              out_sql_cpy='../data/source/out/src-cpy.sql',
                              out_sql_del='../data/source/out/src-del.sql',
                              preserve_ram=True)

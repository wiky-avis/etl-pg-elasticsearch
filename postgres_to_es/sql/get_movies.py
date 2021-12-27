SQL = """
    WITH x as (
    SELECT fw.id, array_agg(pfw.id) as actors_ids, array_agg(p.full_name) as actors_names FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw on fw.id = pfw.film_work_id and pfw.role = 'actor'
    LEFT JOIN content.person p on pfw.person_id = p.id
    where p.full_name is not null
    GROUP BY fw.id
    ), y as(
    SELECT fw.id, array_agg(pfw.id) as directors_ids, array_agg(p.full_name) as directors_names FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw on fw.id = pfw.film_work_id and pfw.role = 'director'
    LEFT JOIN content.person p on pfw.person_id = p.id
    where p.full_name is not null
    GROUP BY fw.id
    ), z as(
    SELECT fw.id, array_agg(pfw.id) as writers_ids, array_agg(p.full_name) as writers_names
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw on fw.id = pfw.film_work_id
    LEFT JOIN content.person p on pfw.person_id = p.id and pfw.role = 'writer'
    where p.full_name is not null
    GROUP BY fw.id
    ), g as(
    SELECT fw.id, array_agg(gfw.id) as genres_ids, array_agg(g.name) as genres_names FROM content.film_work fw
    LEFT JOIN content.genre_film_work gfw on fw.id = gfw.film_work_id
    LEFT JOIN content.genre g on gfw.genre_id = g.id
    GROUP BY fw.id
    )
    SELECT fw.id, title, description, rating, x.actors_ids, x.actors_names, y.directors_ids, y.directors_names, z.writers_ids, z.writers_names, g.genres_ids, g.genres_names
    FROM content.film_work fw 
    LEFT JOIN x ON fw.id = x.id
    LEFT JOIN y ON fw.id = y.id
    LEFT JOIN z ON fw.id = z.id
    LEFT JOIN g ON fw.id = g.id
"""

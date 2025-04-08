# #################################################
# USER KNOWLEDGE RETRIEVAL MIXIN
# #################################################
# import itertools


class UserKnowledgeRetrievalMixin:
    """
    Mixin class for managing user knowledge retrieval in a Loki database.

    IMPLEMENTED METHODS:
    - [getUserSourceID]:
        Retrieves the `source_id` of a specific source in the `user.source`
        table.
    - [getUserSourceIDs]:
        Retrieves `source_id`s for a list of sources or for all sources in the
        `user.source` table if no list is provided

    UTILITY:
    - This mixin class provides methods for efficiently retrieving source IDs
    from the `user.source` table, supporting both targeted lookups and full
    retrievals.
    """

    def getUserSourceID(self, source):
        """
        Retrieves the `source_id` of a specific source in the `user.source`
        table.

        Parameters:
        - source: Name of the source as a string.

        Returns:
        - The `source_id` corresponding to the given source, or None if the
        source is not found.

        Note:
        - This is a helper method that uses `getUserSourceIDs` to perform the
        lookup and directly returns the `source_id` of the requested source.
        """

        return self.getUserSourceIDs([source])[source]

    def getUserSourceIDs(self, sources=None):
        """
        Retrieves `source_id`s for a list of sources or for all sources in the
        `user.source` table if no list is provided.

        Parameters:
        - sources: Optional list of source names for which to retrieve
        `source_id`s.
                If `None`, retrieves all available `source_id`s from the
                `user.source` table.

        Operation:
        - If `sources` is provided:
            - Executes an SQL query to fetch `source_id`s matching the
            provided names, with a case-insensitive comparison.
        - If `sources` is not provided:
            - Executes an SQL query to fetch all `(source, source_id)` pairs
            from the `user.source` table.

        Returns:
        - A dictionary `{source: source_id, ...}` mapping each source to its
        respective `source_id`, or `None` for sources not found.

        Utility:
        - This method efficiently retrieves multiple `source_id`s at once,
        supporting both targeted lookups and full retrievals.
        """

        cursor = self._loki._biofilter.db.cursor()
        if sources:
            sql = "SELECT i.source, s.source_id FROM (SELECT ? AS source) AS i LEFT JOIN `user`.`source` AS s ON LOWER(s.source) = LOWER(i.source)"  # noqa E501
            # ret = {
            #     row[0]: row[1]
            #     for row in cursor.executemany(sql, itertools.izip(sources))
            # }
            ret = {}
            for source in sources:
                cursor.execute(sql, (source,))
                row = cursor.fetchone()
                ret[source] = row[1] if row else None
        else:
            sql = "SELECT source, source_id FROM `user`.`source`"
            ret = {row[0]: row[1] for row in cursor.execute(sql)}
        return ret

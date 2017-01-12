Dummy Dictionary service for use in development of Concept Dictionary Manager at https://gitlab.mitre.org/CDC-SDP/concept-dictionary-manager

Currently requires manual initialization of postgres user/database/codes table.

The user to add is `testsystem`, the database to create is `test_system`, and the table to create is `codes` with a text field for `code`.

In the future we should add something like https://github.com/mattes/migrate to handle migrations.
"""delete

Revision ID: e8dbcc0d7be1
Revises: 73b4be4a60b9
Create Date: 2023-05-30 21:30:37.043771

"""
import logging

from alembic import op


# revision identifiers, used by Alembic.
revision = 'e8dbcc0d7be1'
down_revision = '73b4be4a60b9'
branch_labels = None
depends_on = None


def upgrade():
    logger = logging.getLogger('alembic.env')
    logger.info("Clear old data...")

    op.drop_table('zno_result')

    logger.info("Migrating was done!")

def downgrade():
    pass

"""added timestamp

Revision ID: 7d615a3dac9
Revises: 
Create Date: 2015-08-11 13:28:02.632561

"""

# revision identifiers, used by Alembic.
revision = '7d615a3dac9'
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_paperdata():
    ### commands auto generated by Alembic - please adjust! ###
    #op.drop_table('paperdata')
    op.add_column('feed', sa.Column('timestamp', sa.BigInteger(), nullable=True))
    op.add_column('file', sa.Column('timestamp', sa.BigInteger(), nullable=True))
    op.create_foreign_key(None, 'file', 'observation', ['obsnum'], ['obsnum'])
    op.add_column('observation', sa.Column('timestamp', sa.BigInteger(), nullable=True))
    ### end Alembic commands ###


def downgrade_paperdata():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('observation', 'timestamp')
    op.drop_constraint(None, 'file', type_='foreignkey')
    op.drop_column('file', 'timestamp')
    op.drop_column('feed', 'timestamp')
    #op.create_table('paperdata',
    #sa.Column('path', mysql.VARCHAR(length=100), nullable=True),
    #sa.Column('era', mysql.INTEGER(display_width=11), server_default=sa.text(u"'0'"), autoincrement=False, nullable=True),
    #sa.Column('era_type', mysql.VARCHAR(length=100), nullable=True),
    #sa.Column('obsnum', mysql.BIGINT(display_width=20), server_default=sa.text(u"'0'"), autoincrement=False, nullable=True),
    #sa.Column('md5sum', mysql.VARCHAR(length=32), nullable=True),
    #sa.Column('julian_day', mysql.INTEGER(display_width=11), server_default=sa.text(u"'0'"), autoincrement=False, nullable=True),
    #sa.Column('julian_date', mysql.DECIMAL(precision=12, scale=5), server_default=sa.text(u"'0.00000'"), nullable=True),
    #sa.Column('polarization', mysql.VARCHAR(length=4), nullable=True),
    #sa.Column('data_length', mysql.DECIMAL(precision=6, scale=5), server_default=sa.text(u"'0.00000'"), nullable=True),
    #sa.Column('raw_path', mysql.VARCHAR(length=100), nullable=True),
    #sa.Column('cal_path', mysql.VARCHAR(length=100), nullable=True),
    #sa.Column('npz_path', mysql.VARCHAR(length=100), nullable=True),
    #sa.Column('final_product_path', mysql.VARCHAR(length=100), nullable=True),
    #sa.Column('tape_index', mysql.VARCHAR(length=100), nullable=True),
    #sa.Column('compr_file_size_MB', mysql.DECIMAL(precision=6, scale=1), server_default=sa.text(u"'0.0'"), nullable=True),
    #sa.Column('raw_file_size_MB', mysql.DECIMAL(precision=10, scale=1), server_default=sa.text(u"'0.0'"), nullable=True),
    #sa.Column('compressed', mysql.TINYINT(display_width=1), server_default=sa.text(u"'0'"), autoincrement=False, nullable=True),
    #sa.Column('edge', mysql.TINYINT(display_width=1), server_default=sa.text(u"'0'"), autoincrement=False, nullable=True),
    #sa.Column('write_to_tape', mysql.TINYINT(display_width=1), autoincrement=False, nullable=True),
    #sa.Column('delete_file', mysql.TINYINT(display_width=1), server_default=sa.text(u"'0'"), autoincrement=False, nullable=True),
    #sa.Column('restore_history', mysql.VARCHAR(length=255), nullable=True),
    #sa.Column('comments', mysql.TEXT(), nullable=True),
    #sa.Column('compr_md5sum', mysql.VARCHAR(length=32), server_default=sa.text(u"'NULL'"), nullable=True),
    #sa.Column('npz_md5sum', mysql.VARCHAR(length=32), server_default=sa.text(u"'NULL'"), nullable=True),
    #sa.Column('npz_file_size_MB', mysql.DECIMAL(precision=5, scale=1), server_default=sa.text(u"'0.0'"), nullable=True),
    #mysql_default_charset=u'latin1',
    #mysql_engine=u'MyISAM'
    #)
    ### end Alembic commands ###


def upgrade_ganglia():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('filesystem',
    sa.Column('host', sa.String(length=100), nullable=True),
    sa.Column('system', sa.String(length=100), nullable=True),
    sa.Column('total_space', sa.BigInteger(), nullable=True),
    sa.Column('used_space', sa.BigInteger(), nullable=True),
    sa.Column('free_space', sa.BigInteger(), nullable=True),
    sa.Column('time_date', sa.BigInteger(), nullable=True),
    sa.PrimaryKeyConstraint('host', 'system', 'time_date', name='host_system_time')
    )
    ### end Alembic commands ###


def downgrade_ganglia():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('filesystem')
    ### end Alembic commands ###


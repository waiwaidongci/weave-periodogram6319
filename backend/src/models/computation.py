from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String,Double,JSON,Text,DateTime,Index,text
from datetime import datetime,timezone
from src.config.database import Base
class SpectrumComputation(Base):
 __tablename__='spectrum_computations'
 id:Mapped[int]=mapped_column(primary_key=True)
 fingerprint:Mapped[str]=mapped_column(String(64))
 status:Mapped[str]=mapped_column(String(24),default='queued')
 result:Mapped[dict|None]=mapped_column(JSON,nullable=True)
 error:Mapped[str|None]=mapped_column(Text,nullable=True)
 peak_frequency:Mapped[float|None]=mapped_column(Double(asdecimal=False),nullable=True)
 max_power:Mapped[float|None]=mapped_column(Double(asdecimal=False),nullable=True)
 global_p:Mapped[float|None]=mapped_column(Double(asdecimal=False),nullable=True)
 created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc))
 __table_args__=(Index('ix_computations_live','fingerprint',unique=True,postgresql_where=text("status IN ('queued','running','completed')")),)

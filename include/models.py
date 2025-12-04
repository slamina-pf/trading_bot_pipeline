from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Symbol(Base):
    __tablename__ = "symbol"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, unique=True)
    description = Column(String, nullable=True)

    candles = relationship("MarketOCHLV", back_populates="symbol")

class Timeframe(Base):
    __tablename__ = "timeframe"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    duration_minutes = Column(Integer)

    candles = relationship("MarketOCHLV", back_populates="timeframe")

class MarketOCHLV(Base):
    __tablename__ = "market_ochlv"

    timestamp = Column(Integer, primary_key=True, autoincrement=True)
    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Float)

    symbol_id = Column(Integer, ForeignKey("symbol.id"))
    symbol = relationship("Symbol", back_populates="candles")

    timeframe_id = Column(Integer, ForeignKey("timeframe.id"))
    timeframe = relationship("Timeframe", back_populates="candles")

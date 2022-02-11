import sqlalchemy


class Skill(Base):
    __tablename__ = 'skill'

    id_skill = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(String(150), nullable=False)
    position_skill_id = relationship('Position', secondary='position_skill')


class Position(Base):
    __tablename__ = 'position'

    id_position = Column(Integer, primary_key=True, autoincrement=True)
    position_title = Column(String(150), nullable=False)
    description = Column(Text, nullable=True)
    modality = Column(String(50), nullable=False)
    date_position = Column(DateTime, nullable=False)
    activate = Column(Boolean, default=True, nullable=False)
    num_offers = Column(Integer, default=0, nullable=True)
    salary_min = Column(Integer, nullable=True)
    salary_max = Column(Integer, nullable=True)
    salary = Column(Integer, nullable=True)
    currency = Column(String(50), nullable=True)
    bonus = Column(Integer, nullable=True)
    remote = Column(Boolean, default=True, nullable=True)
    position_skill_id = relationship('Skill', secondary='position_skill')
    company_id = Column(Integer, ForeignKey('company.id_company'))
    


class PositionSkill(Base):
    __tablename__ = 'position_skill'


    id_position_skill = Column(Integer, primary_key=True, autoincrement=True)
    position_id = Column(Integer, ForeignKey(
        'position.id_position'), primary_key=True)
    skill_id = Column(Integer, ForeignKey('skill.id_skill'), primary_key=True)



class Company(Base):
    __tablename__ = 'company'



    id_company = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    industry_id = Column(Integer, ForeignKey('industry.id_industry'))
    company_size = Column(Integer, nullable=True)
    ceo = Column(String(50), nullable=False)
    avg_reputation = Column(Float, nullable=True)
    total_ratings = Column(Integer, nullable=True)
    ceo_score = Column(Float, nullable=True)
    website = Column(String(150), nullable=True)
    culture_score = Column(Float, nullable=True)
    work_life_balance = Column(Float, nullable=True)
    stress_level = Column(Float, nullable=True)
    source_id = Column(Integer, ForeignKey('source.id_source'))



class CompanyLocation(Base):
    __tablename__ = 'company_location'


    id_location = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('company.id_company'))
    company = relationship('Company')
    location = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)
    continent = Column(String(50), nullable=False)



class Source(Base):
    __tablename__ = 'source'



    id_source = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(Text, nullable=True)
    url = Column(String(300), nullable=False)
    documentation = Column(String(300), nullable=True)
    company = relationship('Company')



class Industry(Base):
    __tablename__ = 'industry'



id_industry = Column(Integer, primary_key=True, autoincrement=True)
industry = Column(String(50), nullable=False)
description = Column(String(150), nullable=True)
company = relationship('Company')



class UserReview(Base):
    __tablename__ = 'user_review'



id_review = Column(Integer, primary_key=True, autoincrement=True)
company_id = Column(Integer, ForeignKey('company.id_company'))
company = relationship('Company')
title = Column(String(150), nullable=True)
position_user = Column(String(50), nullable=True)
pros = Column(Text, nullable=True)
const = Column(Text, nullable=True)
review_score = Column(Float, nullable=False)
review_date = Column(DateTime, nullable=False)
useful = Column(Integer, nullable=True)
score_culture = Column(Float, nullable=True)
score_perks = Column(Float, nullable=True)
score_work = Column(Float, nullable=True)
score_stress_level = Column(Float, nullable=True)
score_work_life_balance = Column(Float, nullable=True)
source_link = Column(String(150), nullable=True)



class UserInterview(Base):
    __tablename__ = 'user_interview'



id_review = Column(Integer, primary_key=True, autoincrement=True)
company_id = Column(Integer, ForeignKey('company.id_company'))
company = relationship('Company')
title = Column(String(150), nullable=True)
job_position = Column(String(50), nullable=True)
description = Column(Text, nullable=False)
difficult = Column(String(50), nullable=True)

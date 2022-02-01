CREATE TABLE Company(
    Company_id SERIAL PRIMARY KEY,
    Company VARCHAR Not NULL,
    CONSTRAINT Company_UQ UNIQUE (Company)
)


CREATE TABLE Position(
    Position_id SERIAL PRIMARY KEY,
    Position VARCHAR Not NULL,
    CONSTRAINT Position_UQ UNIQUE (Position)
)


CREATE TABLE JobOffer(
    Offer_id SERIAL PRIMARY KEY,
    Position_id INT,
    Company_id INT,
    Offer_location text NOT NULL,
    Date_published text NOT NULL,
    Offer_URL text NOT NULL,
    Offer_Description text NOT NULL,
    Home_URL text,
    Site_Name text NOT NULL,
    MIN_SALARY numeric NOT NULL,
    MAX_SALARY numeric NOT NULL,
    MID_SALARY numeric NOT NULL,
    CURRENCY text NOT NULL,
    SALARY_PERIOD text,
    CONSTRAINT JobOffer_FK_Positon FOREIGN KEY(Position_id) REFERENCES Position(Position_id),
    CONSTRAINT JobOffer_FK_Company FOREIGN KEY(Company_id) REFERENCES Company(Company_id)
)


CREATE PROCEDURE NEW_OFFER(
    position_ VARCHAR,
    company_ VARCHAR,
    location_ text,
    date_ text,
    url_ text,
    description_ text,
    home_url_ text,
    site_name_ text,
    min_salary_ numeric,
    max_salary_ numeric,
    mid_salary_ numeric,
    currency_ text,
    salary_period text
)
language SQL    
as $$
INSERT INTO jobOffer(
        position_id,
        company_id, 
        Offer_location, 
        Date_published, 
        Offer_URL, 
        Offer_description, 
        Home_URL, 
        Site_Name, 
        MIN_SALARY, 
        MAX_SALARY, 
        MID_SALARY, 
        CURRENCY, 
        SALARY_PERIOD
        )
        VALUES (
            (SELECT Position_id from Position where Position = position_ LIMIT 1),
            (SELECT company_id from Company where Company = company_ LIMIT 1),
            location_,
            date_,
            url_,
            description_,
            home_url_,
            site_name_,
            min_salary_,
            max_salary_,
            mid_salary_,
            currency_,
            salary_period);
            
  $$;
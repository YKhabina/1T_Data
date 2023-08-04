CREATE TABLE IF NOT EXISTS public.index_mass (
    user_id BIGINT,
    weight BIGINT,
    height BIGINT
);
INSERT INTO public.index_mass (user_id, weight, height) VALUES
    (1, 75, 175),
    (2, 60, 182),
    (3, 93, 181);

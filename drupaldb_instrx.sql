/*some high level instructions to create a user from within the source db (drupaldb) for this project; Travis shared a user, but it doesn't have CREATE USER permissions: clock_in and pass f00dc00p*/

CREATE USER IF NOT EXISTS 'membership_user'@'10.116.0.%' IDENTIFIED BY 'mem_update';
GRANT SELECT ON drupaldb.* TO 'membership_user'@'10.116.0.%';
FLUSH PRIVILEGES;


-- the private IP of radish: 10.116.0.2
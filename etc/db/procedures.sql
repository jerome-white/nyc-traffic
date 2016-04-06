delimiter \\

CREATE PROCEDURE getnode(OUT nid INT(10) UNSIGNED)
BEGIN

START TRANSACTION;
SELECT id INTO nid FROM operational WHERE assigned = false LIMIT 1;
UPDATE operational SET assigned = true WHERE id = nid;
COMMIT;

END\\

delimiter ;

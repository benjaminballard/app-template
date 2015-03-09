CREATE TABLE helloworld (
  id               INTEGER      NOT NULL,
  msg              VARCHAR(15),
  PRIMARY KEY (id)
);
PARTITION TABLE helloworld ON COLUMN id;


-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES procs.jar;

-- Define procedures
CREATE PROCEDURE PARTITION ON TABLE helloworld COLUMN id FROM CLASS procedures.SelectHelloMsg;


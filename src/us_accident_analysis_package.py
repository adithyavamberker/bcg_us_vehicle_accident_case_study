from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
import pandas as pd
import yaml


def load_csv_data_to_df(spark, file_path):
    """
    Read CSV data
    :param spark: Spark session instance.
    :param file_path: Path to the CSV file to be loaded.
    :return: A DataFrame containing the CSV data.
    """
    try:
      return spark.read.option("inferSchema", "true").csv(file_path, header=True)
    except Exception as e:
        raise Exception(f"Error loading CSV file from {file_path}: {str(e)}")
    

def write_output(df, file_path, write_format):
    """
    Write data frame to csv
    :param write_format: Write file format
    :param df: dataframe
    :param file_path: output file path
    :return: None
    """
    # df = df.coalesce(1)
    try:
      df.repartition(1).write.format(write_format).mode("overwrite").option(
          "header", "true"
      ).save(file_path)
    except Exception as e:
        raise Exception(f"Error writing output to {file_path}: {str(e)}")


class VehicleAccidentAnalysis:
    def __init__(self, spark, config):
        try:
          input_file_paths = config.get("INPUT_FILENAME")
          self.df_charges = load_csv_data_to_df(spark, input_file_paths.get("Charges"))
          self.df_damages = load_csv_data_to_df(spark, input_file_paths.get("Damages"))
          self.df_endorse = load_csv_data_to_df(spark, input_file_paths.get("Endorse"))
          self.df_primary_person = load_csv_data_to_df(
              spark, input_file_paths.get("Primary_Person")
          )
          self.df_units = load_csv_data_to_df(spark, input_file_paths.get("Units"))
          self.df_restrict = load_csv_data_to_df(spark, input_file_paths.get("Restrict"))
        except Exception as e:
          raise Exception(f"Error initializing class with config: {str(e)}")

    def count_male_crashes_greater_than_2(self, output_path, output_format):
        """
        Finds the number of crashes (accidents) in which number of males killed are greater than 2
            Parameters:
            - output_path (str): The file path for the output file.
            - output_format (str): The file format for writing the output.
            Returns:
            - int: The count of crashes in which number of males killed are greater than 2
        """
        try : 
          df = self.df_primary_person.filter(
              self.df_primary_person.PRSN_GNDR_ID == "MALE"
          ).filter(self.df_primary_person.DEATH_CNT > 2)
          write_output(df, output_path, output_format)
          return df.count()
        except Exception as e:
          return e


    def count_two_wheeler_crashes(self, output_path, output_format):
        """
        Finds crashes where the vehicle body type was two wheelers.

        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - int: The count of crashes involving 2-wheeler vehicles.
        """
        try:
          df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
          write_output(df, output_path, output_format)
          return df.count()
        except Exception as e:
          return e

    def top_5_vehicle_makes_for_fatal_crashes_without_airbags(
        self, output_path, output_format
    ):
        """
        Determines the top 5 Vehicle Makes of the cars involved in crashes where the driver died and airbags did not
        deploy.
        Parameters: - output_format (str): The file format for writing the output. - output_path (str): The
        file path for the output file.
        Returns: - List[str]: Top 5 vehicles Make for killed crashes without an airbag
        deployment.

        """
        try:
          df = (
              self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
              .filter(
                  (col("PRSN_INJRY_SEV_ID") == "KILLED")
                  & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
                  & (col("VEH_MAKE_ID") != "NA")
              )
              .groupby("VEH_MAKE_ID")
              .count()
              .orderBy(col("count").desc())
              .limit(5)
          )
          write_output(df, output_path, output_format)

          return [row[0] for row in df.collect()]
        except Exception as e:
          return e

    def count_hit_and_run_with_valid_licenses(self, output_path, output_format):
        """
        Determines the number of vehicles with drivers having valid licenses involved in hit-and-run incidents.
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - int: The count of vehicles involved in hit-and-run incidents with drivers holding valid licenses.
        """
        try:
          df = (
              self.df_units.select("CRASH_ID", "VEH_HNR_FL")
              .join(
                  self.df_primary_person.select("CRASH_ID", "DRVR_LIC_TYPE_ID"),
                  on=["CRASH_ID"],
                  how="inner",
              )
              .filter(
                  (col("VEH_HNR_FL") == "Y")
                  & (
                      col("DRVR_LIC_TYPE_ID").isin(
                          ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                      )
                  )
              )
          )

          write_output(df, output_path, output_format)

          return df.count()
        except Exception as e:
          return e

    def find_state_with_no_female_accident(self, output_path, output_format):
        """
        Which state has highest number of accidents in which females are not involved?
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - str: The state with the highest number of accidents without female involvement.
        """
        try:
          df = (
              self.df_primary_person.filter(
                  self.df_primary_person.PRSN_GNDR_ID != "FEMALE"
              )
              .groupby("DRVR_LIC_STATE_ID")
              .count()
              .orderBy(col("count").desc())
          )
          write_output(df, output_path, output_format)

          return df.first().DRVR_LIC_STATE_ID
        except Exception as e:
          return e

    def get_top_3to5_vehicle_id_contributing_to_injuries(self, output_path, output_format):
        """
        Finds the VEH_MAKE_IDs ranking from the 3rd to the 5th positions that contribute to the largest number of
        injuries, including death.
        Parameters: - output_format (str): The file format for writing the output. -
        output_path (str): The file path for the output file.
        Returns: - List[int]: The Top 3rd to 5th VEH_MAKE_IDs
        that contribute to the largest number of injuries, including death.
        """
        try:
          df = (
              self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA")
              .withColumn("TOT_CASUALTIES_CNT", self.df_units[35] + self.df_units[36])
              .groupby("VEH_MAKE_ID")
              .sum("TOT_CASUALTIES_CNT")
              .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG")
              .orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
          )

          df_top_3_to_5 = df.limit(5).subtract(df.limit(2))
          write_output(df_top_3_to_5, output_path, output_format)
          
          return [veh[0] for veh in df_top_3_to_5.select("VEH_MAKE_ID").collect()]
        except Exception as e:
          return e


    def display_top_ethnic_ug_by_body_style(self, output_path, output_format):
        """
        Finds and displays the top ethnic user group for each unique body style involved in crashes.
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - dataframe: Displays the top ethnic user group for each unique body style involved in crashes.
        """
        try:
          w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
          df = (
              self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
              .filter(
                  ~self.df_units.VEH_BODY_STYL_ID.isin(
                      ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                  )
              )
              .filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))
              .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
              .count()
              .withColumn("row", row_number().over(w))
              .filter(col("row") == 1)
              .drop("row", "count")
          )

          write_output(df, output_path, output_format)

          return df
        except Exception as e:
          return e

    def find_top_zip_codes_alcohol_crashes(
        self, output_path, output_format
    ):
        """
        Finds the top 5 Zip Codes with the highest number of crashes where alcohol is a contributing factor.
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - List[str]: The top 5 Zip Codes with the highest number of alcohol-related crashes.

        """
        try:
          df = (
              self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
              .dropna(subset=["DRVR_ZIP"])
              .filter(
                  col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")
                  | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
              )
              .groupby("DRVR_ZIP")
              .count()
              .orderBy(col("count").desc())
              .limit(5)
          )
          write_output(df, output_path, output_format)

          return [row[0] for row in df.collect()]
        except Exception as e:
          return e

    def count_crash_ids_with_no_damage(self, output_path, output_format):
        """
        Counts distinct Crash IDs where no damaged property was observed, the damage level (VEH_DMAG_SCL) is above 4,
        and the car has insurance.
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - int: Count distinct Crash IDs meeting the specified criteria.
        """
        try:
          df = (
              self.df_damages.join(self.df_units, on=["CRASH_ID"], how="inner")
              .filter(
                  (
                      (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                      & (
                          ~self.df_units.VEH_DMAG_SCL_1_ID.isin(
                              ["NA", "NO DAMAGE", "INVALID VALUE"]
                          )
                      )
                  )
                  | (
                      (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4")
                      & (
                          ~self.df_units.VEH_DMAG_SCL_2_ID.isin(
                              ["NA", "NO DAMAGE", "INVALID VALUE"]
                          )
                      )
                  )
              )
              .filter(self.df_damages.DAMAGED_PROPERTY == "NONE")
              .filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
          )
          write_output(df, output_path, output_format)

          return df.select('CRASH_ID').distinct().count()
        except Exception as e:
          return e

    def get_top_5_vehicle_makes_speeding_offenses(self, output_path, output_format):
        """
        Determines the top 5 Vehicle Makes/Brands where drivers are charged with speeding-related offences,
        have licensed drivers, use the top 10 used vehicle colours, and have cars licensed with the top 25 states
        with the highest number of offences.
        Parameters: - output_format (str): The file format for writing the
        output. - output_path (str): The file path for the output file. Returns: - List[str]: The list of top 5
        Vehicle Makes/Brands meeting the specified criteria.
        """
        try:
          top_25_state_list = [
              row[0]
              for row in self.df_units.filter(
                  col("VEH_LIC_STATE_ID").cast("int").isNull()
              )
              .groupby("VEH_LIC_STATE_ID")
              .count()
              .orderBy(col("count").desc())
              .limit(25)
              .collect()
          ]
          top_10_used_vehicle_colors = [
              row[0]
              for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA")
              .groupby("VEH_COLOR_ID")
              .count()
              .orderBy(col("count").desc())
              .limit(10)
              .collect()
          ]

          df = (
              self.df_charges.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
              .join(self.df_units, on=["CRASH_ID"], how="inner")
              .filter(self.df_charges.CHARGE.contains("SPEED"))
              .filter(
                  self.df_primary_person.DRVR_LIC_TYPE_ID.isin(
                      ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                  )
              )
              .filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
              .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list))
              .groupby("VEH_MAKE_ID")
              .count()
              .orderBy(col("count").desc())
              .limit(5)
          )

          write_output(df, output_path, output_format)

          return [row[0] for row in df.collect()]
        except Exception as e:
          return e

package org.howard.edu.lsp.assignment2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class ETLPipeline {
    public static void main(String[] args) {
        String inputPath = "data/products.csv";
        String outputPath = "data/transformed_products.csv";

        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.out.println("Error: Input file not found: " + inputPath);
            return;
        }

        int rowsRead = 0;
        int rowsTransformed = 0;
        int rowsSkipped = 0;

        try (
            BufferedReader reader = new BufferedReader(new FileReader(inputPath));
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))
        ) {
            writer.write("ProductID,Name,Price,Category,PriceRange");
            writer.newLine();

            String line = reader.readLine();

            if (line == null) {
                System.out.println("Rows read: 0");
                System.out.println("Rows transformed: 0");
                System.out.println("Rows skipped: 0");
                System.out.println("Output written to: " + outputPath);
                return;
            }

            while ((line = reader.readLine()) != null) {
                rowsRead++;

                if (line.trim().isEmpty()) {
                    rowsSkipped++;
                    continue;
                }

                String[] parts = line.split(",", -1);
                if (parts.length != 4) {
                    rowsSkipped++;
                    continue;
                }

                String productIdText = parts[0].trim();
                String name = parts[1].trim();
                String priceText = parts[2].trim();
                String category = parts[3].trim();

                int productId;
                BigDecimal originalPrice;

                try {
                    productId = Integer.parseInt(productIdText);
                    originalPrice = new BigDecimal(priceText);
                } catch (NumberFormatException e) {
                    rowsSkipped++;
                    continue;
                }

                String transformedName = name.toUpperCase();
                String originalCategory = category;

                BigDecimal finalPrice = originalPrice;
                if (originalCategory.equals("Electronics")) {
                    finalPrice = originalPrice.multiply(new BigDecimal("0.90"));
                }

                finalPrice = finalPrice.setScale(2, RoundingMode.HALF_UP);

                String finalCategory = originalCategory;
                if (originalCategory.equals("Electronics")
                        && finalPrice.compareTo(new BigDecimal("500.00")) > 0) {
                    finalCategory = "Premium Electronics";
                }

                String priceRange;
                if (finalPrice.compareTo(new BigDecimal("10.00")) <= 0) {
                    priceRange = "Low";
                } else if (finalPrice.compareTo(new BigDecimal("100.00")) <= 0) {
                    priceRange = "Medium";
                } else if (finalPrice.compareTo(new BigDecimal("500.00")) <= 0) {
                    priceRange = "High";
                } else {
                    priceRange = "Premium";
                }

                writer.write(productId + "," + transformedName + "," + finalPrice.toPlainString()
                        + "," + finalCategory + "," + priceRange);
                writer.newLine();
                rowsTransformed++;
            }

            System.out.println("Rows read: " + rowsRead);
            System.out.println("Rows transformed: " + rowsTransformed);
            System.out.println("Rows skipped: " + rowsSkipped);
            System.out.println("Output written to: " + outputPath);

        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
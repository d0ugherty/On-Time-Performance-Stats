# Set the path to the directory containing the CSV files
Write-Output "Setting path..."
$csvPath = "data\"

# Set the path and name for the combined CSV file
$combinedCsvPath = "data\OTP_combined.csv"

# Get all CSV files in the directory
Write-Output "Getting CSV files.."
$csvFiles = Get-ChildItem -Path $csvPath -Filter *.csv

# Create an empty array to store the combined CSV data
$combinedData = @()

Write-Output "Combining CSV files..."
# Loop through each CSV file and append its data to the combinedData array
foreach ($csvFile in $csvFiles) {
    $csvData = Import-Csv $csvFile.FullName
    $combinedData += $csvData
}

# Export the combinedData array to a new CSV file
$combinedData | Export-Csv $combinedCsvPath -NoTypeInformation

Write-Output "CSV files succesfully combined and exported."
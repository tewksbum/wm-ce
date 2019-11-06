package pipeline

func pipelineParse(input InputRecord) (output *OutputRecord, err error) {
	var mkOutput IdentifiedRecord
	var trustedID string

	for _, column := range input.Columns {
		if column.ERR.TrustedID == 1 {
			trustedID = column.Value
		}
		if column.ERR.CampaignID == 1 {
			mkOutput.CampaignID = column.Value
		}
		if column.ERR.Name == 1 {
			mkOutput.Name = column.Value
		}
		if column.ERR.Type == 1 {
			mkOutput.Type = column.Value
		}
		if column.ERR.Channel == 1 {
			mkOutput.Channel = column.Value
		}
		if column.ERR.Budget == 1 {
			mkOutput.Budget = column.Value
		}
		if column.ERR.StartDate == 1 {
			mkOutput.StartDate = column.Value
		}
		if column.ERR.EndDate == 1 {
			mkOutput.EndDate = column.Value
		}
	}

	// assemble output
	output = new(OutputRecord)
	if trustedID != "" {
		outputTrustedID := OutputTrustedID{
			Source:   input.Source,
			SourceID: trustedID,
		}
		output.TrustedID = append(output.TrustedID, outputTrustedID)
	}
	output.Owner = input.Owner
	output.Request = input.Request
	output.Source = input.Source
	output.Row = input.Row
	output.TimeStamp = input.TimeStamp
	output.Record = mkOutput

	// mkJSON, _ := json.Marshal(mkOutput)
	// log.Printf("MatchKey Columns after Clean up %v", string(mkJSON))

	return output, err
}

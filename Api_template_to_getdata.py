
****************template for the api for get method to get fieldds from the dynmodb************************************
#set($inputRoot = $input.path('$'))
#if ($inputRoot.Count > 0)
#set($campaignDataFound = false)
#set($flag = 0)
[
    #foreach($elem in $inputRoot.Items)
        #if($elem.Record_Status.S == "A")
            #set($campaignDataFound = true)
            #if($flag >0),
            #end
            {
                "y": "$elem.y.S",
                "Activation_Phrase": "$elem.Activation_Phrase.S",
                "Delivery_date": "$elem.Delivery_date.S",
                "Campaign_duration_time_hours": "$elem.Campaign_duration_time_hours.S",
                "Queue_name": "$elem.Queue_name.S",
                "Campaign_ID": "$elem.Campaign_ID.S",
                "Opco": "$elem.Opco.S",
                "textbody": "$elem.textbody.S"
                
            }
            #set($flag = $flag+1)   
            
        #end
    #end
    #if(!$campaignDataFound)
    {
        "msg": "Record Status not A!"
    }
    #end
]
#else
{
    "msg": "Campaign Data not found!"
}
#end
